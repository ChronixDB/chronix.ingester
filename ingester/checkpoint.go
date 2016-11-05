package ingester

import (
	"bufio"
	"encoding/binary"
	"os"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/local/chunk"
	"github.com/prometheus/prometheus/storage/local/codable"
)

const fileBufSize = 1 << 16 // 64kiB.

type checkpointer struct {
	fileName string
}

// newCheckpointer returns a checkpointer that checkpoints to local disk.
func newCheckpointer(fileName string) *checkpointer {
	p := &checkpointer{
		fileName: fileName,
	}

	return p
}

// checkpoint persists the fingerprint to memory-series mapping
// and all non-persisted chunks.
//
// Description of the file format:
//
// (1) Number of series in checkpoint as big-endian uint64.
//
// (2) Repeated once per series:
//
// (2.1) The metric as defined by codable.Metric.
//
// (2.2) The varint-encoded number of chunk descriptors.
//
// (2.3) Repeated once per chunk descriptor, oldest to most recent:
//
// (2.3.1) A byte defining the chunk type.
//
// (2.3.2) The chunk itself, marshaled with the Marshal() method.
//
func (c *checkpointer) checkpoint(fingerprintToSeries *seriesMap, fpLocker *fingerprintLocker) (err error) {
	f, err := os.OpenFile(c.tempFileName(), os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0640)
	if err != nil {
		return err
	}

	defer func() {
		syncErr := f.Sync()
		closeErr := f.Close()
		if err != nil {
			return
		}
		err = syncErr
		if err != nil {
			return
		}
		err = closeErr
		if err != nil {
			return
		}
		err = os.Rename(c.tempFileName(), c.fileName)
	}()

	w := bufio.NewWriterSize(f, fileBufSize)

	numberOfSeriesInHeader := uint64(fingerprintToSeries.length())
	// We have to write the number of series as uint64 because we might need
	// to overwrite it later, and a varint might change byte width then.
	if err = codable.EncodeUint64(w, numberOfSeriesInHeader); err != nil {
		return err
	}

	iter := fingerprintToSeries.iter()
	defer func() {
		// Consume the iterator in any case to not leak goroutines.
		for range iter {
		}
	}()

	var realNumberOfSeries uint64
	for m := range iter {
		func() { // Wrapped in function to use defer for unlocking the fp.
			fpLocker.Lock(m.fp)
			defer fpLocker.Unlock(m.fp)

			if len(m.series.chunkDescs) == 0 {
				// This series was completely persisted and removed from memory in the meantime. Ignore.
				return
			}
			realNumberOfSeries++

			var buf []byte
			buf, err = codable.Metric(m.series.metric).MarshalBinary()
			if err != nil {
				return
			}
			if _, err = w.Write(buf); err != nil {
				return
			}
			if _, err = codable.EncodeVarint(w, int64(len(m.series.chunkDescs))); err != nil {
				return
			}
			for _, chunkDesc := range m.series.chunkDescs {
				if err = chunkDesc.C.Marshal(w); err != nil {
					return
				}
			}
		}()
		if err != nil {
			return err
		}
	}
	if err = w.Flush(); err != nil {
		return err
	}
	if realNumberOfSeries != numberOfSeriesInHeader {
		// The number of series has changed in the meantime.
		// Rewrite it in the header.
		if _, err = f.Seek(0, os.SEEK_SET); err != nil {
			return err
		}
		if err = codable.EncodeUint64(f, realNumberOfSeries); err != nil {
			return err
		}
	}
	return err
}

// recover reads back and recovers from a checkpoint as written out by checkpoint().
func (c *checkpointer) recover(sm *seriesMap, mapper *fpMapper) (numMemoryChunks int, err error) {
	f, err := os.Open(c.fileName)
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, fileBufSize)

	seriesTotal, err := codable.DecodeUint64(r)
	if err != nil {
		return 0, err
	}

	var metric codable.Metric
	var numChunkDescs int64
	for i := uint64(0); i < seriesTotal; i++ {
		err = metric.UnmarshalFromReader(r)
		if err != nil {
			return 0, err
		}
		numChunkDescs, err = binary.ReadVarint(r)
		if err != nil {
			return 0, err
		}
		chunkDescs := make([]*chunk.Desc, numChunkDescs)
		for i := int64(0); i < numChunkDescs; i++ {
			ch := chunk.New()
			if err = ch.Unmarshal(r); err != nil {
				return 0, err
			}
			cd := chunk.NewDesc(ch, ch.FirstTime())
			numMemoryChunks++
			chunkDescs[i] = cd
		}

		lastTimeHead, err := chunkDescs[len(chunkDescs)-1].LastTime()
		if err != nil {
			return 0, err
		}

		met := model.Metric(metric)
		fp := met.FastFingerprint()
		sm.m[fp] = &memorySeries{
			metric:     model.Metric(metric),
			chunkDescs: chunkDescs,
			lastTime:   lastTimeHead,
		}
	}
	return numMemoryChunks, nil
}

func (c *checkpointer) tempFileName() string {
	return c.fileName + ".tmp"
}
