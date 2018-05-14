package chronix

import (
	"errors"
)

type stats struct {
	count int64
	min float64
	max float64
	avg float64
	timespan int64
}

// Calculates the statistics for one TimeSeries
func calculateStats(timeSeries *TimeSeries) (stats, error) {
	points := &timeSeries.Points

	result := stats{}

	if len(*points) == 0 {
		return stats{}, errors.New("TimeSeries has no Points")
	}

	result.count = int64(len(*points))
	result.min = (*points)[0].Value
	result.max = (*points)[0].Value

	var sum float64 = 0

	for _, point := range *points {
		if point.Value > result.max {
			result.max = point.Value
		} else if point.Value < result.min {
			result.min = point.Value
		}
		sum += point.Value
	}

	result.avg = sum / float64(result.count)
	result.timespan = (*points)[len(*points) - 1].Timestamp - (*points)[0].Timestamp

	return result, nil
}
