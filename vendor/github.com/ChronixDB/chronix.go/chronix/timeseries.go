package chronix

// A TimeSeries models a Chronix time series chunk.
type TimeSeries struct {
	Name     string
	Type       string
	Attributes map[string]string
	Points     []Point
}

// A Point models a Chronix time series sample.
type Point struct {
	Timestamp int64
	Value     float64
}
