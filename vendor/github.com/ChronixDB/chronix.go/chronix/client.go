package chronix

import (
	"encoding/base64"
	"fmt"
	"time"
)

// Client is a client that allows storing time series in Chronix.
type Client interface {
	Store(ts []*TimeSeries, commit bool, commitWithin time.Duration) error
	// TODO: Return a more interpreted query result on the Chronix level.
	Query(q, fq, fl string) ([]byte, error)
}

type client struct {
	solr SolrClient
}

// New creates a new Chronix client.
func New(s SolrClient) Client {
	return &client{
		solr: s,
	}
}

func (c *client) Store(series []*TimeSeries, commit bool, commitWithin time.Duration) error {
	if len(series) == 0 {
		return nil
	}

	update := []map[string]interface{}{}
	for _, ts := range series {
		if len(ts.Points) == 0 {
			continue
		}

		data, err := encode(ts.Points, 0)
		if err != nil {
			return fmt.Errorf("error encoding points: %v", err)
		}
		encData := base64.StdEncoding.EncodeToString(data)
		fields := map[string]interface{}{
			"start":  ts.Points[0].Timestamp,
			"end":    ts.Points[len(ts.Points)-1].Timestamp,
			"data":   encData,
			"metric": ts.Metric,
		}

		for k, v := range ts.Attributes {
			fields[k+"_s"] = v
		}

		update = append(update, fields)
	}
	return c.solr.Update(update, commit, commitWithin)
}

func (c *client) Query(q, fq, fl string) ([]byte, error) {
	return c.solr.Query(q, fq, fl)
}
