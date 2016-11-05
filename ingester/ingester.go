package ingester

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	prom_chunk "github.com/prometheus/prometheus/storage/local/chunk"
)

const (
	discardReasonLabel = "reason"

	// Reasons to discard samples.
	outOfOrderTimestamp = "timestamp_out_of_order"
	duplicateSample     = "multiple_values_for_timestamp"
)

var (
	memorySeriesDesc = prometheus.NewDesc(
		"chronix_ingester_memory_series",
		"The current number of series in memory.",
		nil, nil,
	)

	// ErrOutOfOrderSample is returned if a sample has a timestamp before the latest
	// timestamp in the series it is appended to.
	ErrOutOfOrderSample = fmt.Errorf("sample timestamp out of order")
	// ErrDuplicateSampleForTimestamp is returned if a sample has the same
	// timestamp as the latest sample in the series it is appended to but a
	// different value. (Appending an identical sample is a no-op and does
	// not cause an error.)
	ErrDuplicateSampleForTimestamp = fmt.Errorf("sample with repeated timestamp but different value")
)

// A ChunkStore writes Prometheus chunks to a backing store.
type ChunkStore interface {
	Put(model.Metric, []*prom_chunk.Desc) error
}

// Config configures an Ingester.
type Config struct {
	FlushCheckPeriod     time.Duration
	MaxChunkAge          time.Duration
	MaxConcurrentFlushes int
	CheckpointFile       string
	CheckpointInterval   time.Duration
	FlushOnShutdown      bool
}

// An Ingester batches up samples for multiple series and stores
// them as chunks in a ChunkStore.
type Ingester struct {
	// Configuration and lifecycle management.
	cfg          Config
	chunkStore   ChunkStore
	checkpointer *checkpointer
	stopLock     sync.RWMutex
	stopped      bool
	quit         chan struct{}
	done         chan struct{}

	// Sample ingestion state.
	fpLocker   *fingerprintLocker
	fpToSeries *seriesMap
	mapper     *fpMapper

	// Metrics about the Ingester itself.
	ingestedSamples    prometheus.Counter
	discardedSamples   *prometheus.CounterVec
	chunkUtilization   prometheus.Histogram
	chunkStoreFailures prometheus.Counter
	memoryChunks       prometheus.Gauge
	checkpointDuration prometheus.Gauge
}

// NewIngester constructs a new Ingester.
func NewIngester(cfg Config, chunkStore ChunkStore) (*Ingester, error) {
	if cfg.FlushCheckPeriod == 0 {
		cfg.FlushCheckPeriod = 1 * time.Minute
	}
	if cfg.MaxChunkAge == 0 {
		cfg.MaxChunkAge = 30 * time.Minute
	}
	if cfg.MaxConcurrentFlushes == 0 {
		cfg.MaxConcurrentFlushes = 100
	}

	i := &Ingester{
		cfg:        cfg,
		chunkStore: chunkStore,
		quit:       make(chan struct{}),
		done:       make(chan struct{}),

		fpLocker: newFingerprintLocker(16),

		ingestedSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronix_ingester_ingested_samples_total",
			Help: "The total number of samples ingested.",
		}),
		discardedSamples: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "chronix_ingester_out_of_order_samples_total",
				Help: "The total number of samples that were discarded because their timestamps were at or before the last received sample for a series.",
			},
			[]string{discardReasonLabel},
		),
		chunkUtilization: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "chronix_ingester_chunk_utilization",
			Help:    "Distribution of stored chunk utilization.",
			Buckets: []float64{0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9},
		}),
		memoryChunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronix_ingester_memory_chunks",
			Help: "The total number of chunks in memory.",
		}),
		chunkStoreFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "chronix_ingester_chunk_store_failures_total",
			Help: "The total number of errors while storing chunks to the chunk store.",
		}),
		checkpointDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "chronix_ingester_checkpoint_duration_seconds",
			Help: "The duration in seconds it took to checkpoint in-memory chunks and metrics.",
		}),
	}

	i.checkpointer = newCheckpointer(cfg.CheckpointFile)
	i.fpToSeries = newSeriesMap()
	i.mapper = newFPMapper(i.fpToSeries)

	log.Info("Recovering from checkpoint...")
	numMemoryChunks, err := i.checkpointer.recover(i.fpToSeries, i.mapper)
	if err != nil {
		return nil, fmt.Errorf("error loading checkpoint: %v", err)
	}
	log.Infof("Recovered %d series with %d chunks from checkpoint.", len(i.fpToSeries.m), numMemoryChunks)

	i.memoryChunks.Set(float64(numMemoryChunks))

	go i.loop()
	return i, nil
}

// NeedsThrottling implements storage.SampleAppender.
func (*Ingester) NeedsThrottling() bool {
	// Always return false for now - this method is only there to implement the interface.
	return false
}

// Append implements storage.SampleAppender.
func (i *Ingester) Append(sample *model.Sample) error {
	for ln, lv := range sample.Metric {
		if len(lv) == 0 {
			delete(sample.Metric, ln)
		}
	}

	i.stopLock.RLock()
	defer i.stopLock.RUnlock()
	if i.stopped {
		return fmt.Errorf("ingester stopping")
	}

	fp, series := i.getOrCreateSeries(sample.Metric)
	defer func() {
		i.fpLocker.Unlock(fp)
	}()

	if sample.Timestamp == series.lastTime {
		// Don't report "no-op appends", i.e. where timestamp and sample
		// value are the same as for the last append, as they are a
		// common occurrence when using client-side timestamps
		// (e.g. Pushgateway or federation).
		if sample.Timestamp == series.lastTime &&
			series.lastSampleValueSet &&
			sample.Value.Equal(series.lastSampleValue) {
			return nil
		}
		i.discardedSamples.WithLabelValues(duplicateSample).Inc()
		return ErrDuplicateSampleForTimestamp // Caused by the caller.
	}
	if sample.Timestamp < series.lastTime {
		i.discardedSamples.WithLabelValues(outOfOrderTimestamp).Inc()
		return ErrOutOfOrderSample // Caused by the caller.
	}
	prevNumChunks := len(series.chunkDescs)
	_, err := series.add(model.SamplePair{
		Value:     sample.Value,
		Timestamp: sample.Timestamp,
	})
	i.memoryChunks.Add(float64(len(series.chunkDescs) - prevNumChunks))

	if err == nil {
		// TODO: Track append failures too (unlikely to happen).
		i.ingestedSamples.Inc()
	}
	return err
}

func (i *Ingester) getOrCreateSeries(metric model.Metric) (model.Fingerprint, *memorySeries) {
	rawFP := metric.FastFingerprint()
	i.fpLocker.Lock(rawFP)
	fp := i.mapper.mapFP(rawFP, metric)
	if fp != rawFP {
		i.fpLocker.Unlock(rawFP)
		i.fpLocker.Lock(fp)
	}

	series, ok := i.fpToSeries.get(fp)
	if ok {
		return fp, series
	}

	series = newMemorySeries(metric)
	i.fpToSeries.put(fp, series)
	return fp, series
}

// Stop stops the Ingester.
func (i *Ingester) Stop() {
	i.stopLock.Lock()
	i.stopped = true
	i.stopLock.Unlock()

	close(i.quit)
	<-i.done
}

func (i *Ingester) checkpoint() {
	log.Info("Checkpointing unpersisted state...")
	begin := time.Now()
	if err := i.checkpointer.checkpoint(i.fpToSeries, i.fpLocker); err != nil {
		log.Errorln("Error writing checkpoint:", err)
	} else {
		duration := time.Since(begin)
		i.checkpointDuration.Set(duration.Seconds())
		log.Infof("Done checkpointing in %v.", duration)
	}
}

func (i *Ingester) loop() {
	defer func() {
		if i.cfg.FlushOnShutdown {
			i.flushAllSeries(true)
		}
		i.checkpoint()
		close(i.done)
		log.Infoln("Ingester exited gracefully")
	}()

	flushTick := time.Tick(i.cfg.FlushCheckPeriod)
	checkpointTick := time.Tick(i.cfg.CheckpointInterval)
	for {
		select {
		case <-flushTick:
			i.flushAllSeries(false)
		case <-checkpointTick:
			i.checkpoint()
		case <-i.quit:
			return
		}
	}
}

func (i *Ingester) flushAllSeries(immediate bool) {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, i.cfg.MaxConcurrentFlushes)
	for pair := range i.fpToSeries.iter() {
		wg.Add(1)
		semaphore <- struct{}{}
		go func(pair fingerprintSeriesPair) {
			if err := i.flushSeries(pair.fp, pair.series, immediate); err != nil {
				log.Errorf("Failed to flush chunks for series: %v", err)
			}
			<-semaphore
			wg.Done()
		}(pair)
	}
	wg.Wait()
}

func (i *Ingester) flushSeries(fp model.Fingerprint, series *memorySeries, immediate bool) error {
	i.fpLocker.Lock(fp)

	// Decide what chunks to flush.
	if immediate || time.Now().Sub(series.head().FirstTime().Time()) > i.cfg.MaxChunkAge {
		series.headChunkClosed = true
	}
	chunks := series.chunkDescs
	if !series.headChunkClosed {
		chunks = chunks[:len(chunks)-1]
	}
	i.fpLocker.Unlock(fp)
	if len(chunks) == 0 {
		return nil
	}

	// Flush the chunks without locking the series.
	if err := i.chunkStore.Put(series.metric, chunks); err != nil {
		i.chunkStoreFailures.Add(float64(len(chunks)))
		return err
	}
	for _, c := range chunks {
		i.chunkUtilization.Observe(c.C.Utilization())
	}

	// Now remove the chunks.
	i.fpLocker.Lock(fp)
	series.chunkDescs = series.chunkDescs[len(chunks):]
	i.memoryChunks.Sub(float64(len(chunks)))
	if len(series.chunkDescs) == 0 {
		i.fpToSeries.del(fp)
	}
	i.fpLocker.Unlock(fp)
	return nil
}

// Describe implements prometheus.Collector.
func (i *Ingester) Describe(ch chan<- *prometheus.Desc) {
	ch <- memorySeriesDesc
	ch <- i.ingestedSamples.Desc()
	i.discardedSamples.Describe(ch)
	ch <- i.chunkUtilization.Desc()
	ch <- i.chunkStoreFailures.Desc()
	ch <- i.memoryChunks.Desc()
	ch <- i.checkpointDuration.Desc()
}

// Collect implements prometheus.Collector.
func (i *Ingester) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		memorySeriesDesc,
		prometheus.GaugeValue,
		float64(i.fpToSeries.length()),
	)
	ch <- i.ingestedSamples
	i.discardedSamples.Collect(ch)
	ch <- i.chunkUtilization
	ch <- i.chunkStoreFailures
	ch <- i.memoryChunks
	ch <- i.checkpointDuration
}
