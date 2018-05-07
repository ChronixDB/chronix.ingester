package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ChronixDB/chronix.go/chronix"
	"github.com/ChronixDB/chronix.ingester/ingester"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/storage/remote"
)

type erroringChronix struct{}

func (c *erroringChronix) Store(ts []*chronix.TimeSeries, commit bool, commitWithin time.Duration) error {
	return fmt.Errorf("this is a purposefully erroring Chronix client")
}

func (c *erroringChronix) Query(q, fq, fl string) ([]byte, error) {
	panic("not implemented")
}

// A testChronix instance acts as a chronix.Client that records any series sent
// to it and can return them as a model.Matrix.
type testChronix struct {
	mtx           sync.Mutex
	sampleStreams map[model.Fingerprint]*model.SampleStream
}

func (c *testChronix) Store(ts []*chronix.TimeSeries, commit bool, commitWithin time.Duration) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, s := range ts {
		m := model.Metric{
			model.MetricNameLabel: model.LabelValue(s.Name),
		}
		for k, v := range s.Attributes {
			m[model.LabelName(k)] = model.LabelValue(v)
		}

		fp := m.Fingerprint()
		ss, exists := c.sampleStreams[fp]
		if !exists {
			ss = &model.SampleStream{
				Metric: m,
			}
			c.sampleStreams[fp] = ss
		}

		for _, p := range s.Points {
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.TimeFromUnixNano(p.Timestamp * 1e6),
				Value:     model.SampleValue(p.Value),
			})
		}
	}
	return nil
}

func (c *testChronix) Query(q, fq, fl string) ([]byte, error) {
	panic("not implemented")
}

func (c *testChronix) toMatrix() model.Matrix {
	m := make(model.Matrix, 0, len(c.sampleStreams))
	for _, ss := range c.sampleStreams {
		m = append(m, ss)
	}
	return m
}

func buildTestMatrix(numSeries int, samplesPerSeries int) model.Matrix {
	m := make(model.Matrix, 0, numSeries)
	for i := 0; i < numSeries; i++ {
		ss := model.SampleStream{
			Metric: model.Metric{
				model.MetricNameLabel: model.LabelValue(fmt.Sprintf("testmetric_%d", i)),
				model.JobLabel:        "testjob",
			},
			Values: make([]model.SamplePair, 0, samplesPerSeries),
		}
		for j := 0; j < samplesPerSeries; j++ {
			ss.Values = append(ss.Values, model.SamplePair{
				Timestamp: model.Time(i + j),
				Value:     model.SampleValue(i + j),
			})
		}
		m = append(m, &ss)
	}
	sort.Sort(m)
	return m
}

func matrixToSamples(m model.Matrix) []*model.Sample {
	var samples []*model.Sample
	for _, ss := range m {
		for _, sp := range ss.Values {
			samples = append(samples, &model.Sample{
				Metric:    ss.Metric,
				Timestamp: sp.Timestamp,
				Value:     sp.Value,
			})
		}
	}
	return samples
}

func TestEndToEnd(t *testing.T) {
	chronix := &testChronix{
		sampleStreams: map[model.Fingerprint]*model.SampleStream{},
	}
	checkpointFile := "test-checkpoint.db"
	defer os.Remove(checkpointFile)
	ing, err := ingester.NewIngester(
		ingester.Config{
			MaxChunkAge:    9999 * time.Hour,
			CheckpointFile: checkpointFile,
		},
		&chronixStore{chronix: chronix},
	)
	if err != nil {
		t.Fatal(err)
	}

	mux := http.NewServeMux()
	serv := httptest.NewServer(mux)
	defer serv.Close()

	mux.Handle("/", ingestHandler(ing))

	u, err := url.Parse(serv.URL)
	if err != nil {
		panic(err)
	}
	ingClient, err := remote.NewClient(0, &remote.ClientConfig{
		URL:     &config.URL{URL: u},
		Timeout: model.Duration(time.Second),
	})

	// Create test samples.
	testData := buildTestMatrix(10, 1000)

	// Shove test samples into the ingester.
	if err := ingClient.Store(matrixToSamples(testData)); err != nil {
		t.Fatal(err)
	}

	// Stop the ingester, causing it to checkpoint its state to disk.
	ing.Stop()

	// Create a new ingester that recovers from the checkpoint, but tries
	// to store chunks into an erroring Chronix client.
	ing, err = ingester.NewIngester(
		ingester.Config{
			MaxChunkAge:     9999 * time.Hour,
			CheckpointFile:  checkpointFile,
			FlushOnShutdown: true,
		},
		&chronixStore{chronix: &erroringChronix{}},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Stop the ingester, causing it to try and flush its chunks to Chronix.
	// But storing chunks in the erroring Chronix client will fail, so it will
	// still checkpoint all chunks to disk (again).
	ing.Stop()

	// No samples should have been stored in the working Chronix client yet.
	if len(chronix.toMatrix()) != 0 {
		t.Fatal("Unexpected samples were stored in Chronix client:", chronix.toMatrix())
	}

	// Create a new ingester that recovers from the checkpoint again, but talks
	// to a working Chronix client this time.
	ing, err = ingester.NewIngester(
		ingester.Config{
			MaxChunkAge:     9999 * time.Hour,
			CheckpointFile:  checkpointFile,
			FlushOnShutdown: true,
		},
		&chronixStore{chronix: chronix},
	)
	if err != nil {
		t.Fatal(err)
	}

	// Stop the ingester, causing it to flush its chunks to Chronix.
	ing.Stop()

	// Compare stored samples from Chronix with expected samples.
	want := chronix.toMatrix()
	sort.Sort(want)

	if !reflect.DeepEqual(want, testData) {
		t.Fatalf("unexpected stored data\n\nwant:\n\n%v\n\ngot:\n\n%v\n\n", testData, want)
	}
}
