package chronix

import (
	"errors"
	"math/big"
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

	var sum = big.NewFloat(0)

	for _, point := range *points {
		if point.Value > result.max {
			result.max = point.Value
		} else if point.Value < result.min {
			result.min = point.Value
		}
		sum.Add(sum, big.NewFloat(point.Value))
	}

	result.avg, _ = sum.Quo(sum, big.NewFloat(float64(result.count))).Float64()
	result.timespan = (*points)[len(*points) - 1].Timestamp - (*points)[0].Timestamp

	return result, nil
}
