package chronix

import (
	"time"
	"encoding/base64"
	"fmt"
)

// Client is a client that allows storing time series in Chronix.
type Client interface {
	Store(ts []*TimeSeries, commit bool, commitWithin time.Duration) error
	// TODO: Return a more interpreted query result on the Chronix level.
	Query(q, fq, fl string) ([]byte, error)
}

type client struct {
	storage StorageClient
	createStatistics bool
}

// New creates a new Chronix client. The client does not create statistics for the individual data chunks in the storage.
func New(s StorageClient) Client {
	return &client{
		storage: s,
		createStatistics: false,
	}
}

// creates a new Chronix client. The client does create statistics for the individual data chunks in the storage.
func NewWithStatistics(s StorageClient) Client {
	return &client{
		storage: s,
		createStatistics: true,
	}
}

func (c *client) Store(series []*TimeSeries, commit bool, commitWithin time.Duration) error {
	if len(series) == 0 {
		return nil
	}

	var update []map[string]interface{}
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
			"start": ts.Points[0].Timestamp,
			"end":   ts.Points[len(ts.Points)-1].Timestamp,
			"data":  encData,
			"name":  ts.Name,
			"type":  ts.Type,
		}

		if c.storage.NeedPostfixOnDynamicField() {
			for k, v := range ts.Attributes {
				fields[k+"_s"] = v
			}
		} else {
			for k, v := range ts.Attributes {
				fields[k] = v
			}
		}

		err = c.addStatistics(ts, &fields)
		if err != nil {
			return fmt.Errorf("error adding statistics: %v", err)
		}

		update = append(update, fields)
	}
	return c.storage.Update(update, commit, commitWithin)
}

func (c *client) addStatistics(series *TimeSeries, fields *map[string]interface{}) error {
	if !c.createStatistics {
		return nil
	}

	stats, err := calculateStats(series)
	if err != nil {
		return err
	}

	suffix := ""
	if c.storage.NeedPostfixOnDynamicField() {
		suffix = "_f"
	}

	(*fields)["stats_timespan" + suffix] = stats.timespan
	(*fields)["stats_count" + suffix] = stats.count
	(*fields)["stats_min" + suffix] = stats.min
	(*fields)["stats_max" + suffix] = stats.max
	(*fields)["stats_avg" + suffix] = stats.avg
	return nil
}

func (c *client) Query(q, fq, fl string) ([]byte, error) {
	return c.storage.Query(q, fq, fl)
}
