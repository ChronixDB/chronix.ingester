package main

import (
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/storage/remote"
)

// ingestHandler returns an http.Handler that accepts proto encoded samples.
func ingestHandler(appender storage.SampleAppender) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if v := r.Header.Get("X-Prometheus-Remote-Write-Version"); v != "0.1.0" {
			msg := fmt.Sprintf("Unsupported remote write protocol version %q", v)
			log.Errorln(msg)
			http.Error(w, msg, http.StatusBadRequest)
			return
		}

		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Errorf("Error reading request body: %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Errorf("Error decompressing request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req remote.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Errorf("Error unmarshalling request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		for _, ts := range req.Timeseries {
			metric := model.Metric{}
			for _, l := range ts.Labels {
				metric[model.LabelName(l.Name)] = model.LabelValue(l.Value)
			}

			for _, s := range ts.Samples {
				err := appender.Append(&model.Sample{
					Metric:    metric,
					Value:     model.SampleValue(s.Value),
					Timestamp: model.Time(s.TimestampMs),
				})
				if err != nil {
					log.Errorf("Error appending sample: %v", err)
					http.Error(w, err.Error(), http.StatusInternalServerError)
					return
				}
			}
		}
	})
}
