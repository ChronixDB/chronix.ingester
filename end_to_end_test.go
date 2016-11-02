package main

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/ChronixDB/chronix.go/chronix"
	"github.com/ChronixDB/chronix.ingester/ingester"
	"github.com/prometheus/common/model"
)

// A testChronix instance acts as a chronix.Client that records any series sent
// to it and can return them as a model.Matrix.
type testChronix struct {
	mtx           sync.Mutex
	sampleStreams map[model.Fingerprint]*model.SampleStream
}

func (c *testChronix) Store(ts []*chronix.TimeSeries, commit bool) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	for _, s := range ts {
		m := model.Metric{
			model.MetricNameLabel: model.LabelValue(s.Metric),
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
	ing := ingester.NewIngester(
		ingester.Config{
			MaxChunkAge: 9999 * time.Hour,
		},
		&chronixStore{chronix: chronix},
	)

	// Create test samples.
	testData := buildTestMatrix(10, 1000)

	// Shove test samples into the ingester.
	for _, s := range matrixToSamples(testData) {
		err := ing.Append(s)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Stop the ingester, causing it to flush all chunks to Chronix.
	ing.Stop()

	// Compare stored samples from Chronix with expected samples.
	want := chronix.toMatrix()
	sort.Sort(want)

	if !reflect.DeepEqual(want, testData) {
		t.Fatalf("unexpected stored data\n\nwant:\n\n%v\n\ngot:\n\n%v\n\n", testData, want)
	}
}
