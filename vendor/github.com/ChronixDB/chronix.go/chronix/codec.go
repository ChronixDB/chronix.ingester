package chronix

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"math"

	"github.com/ChronixDB/chronix.go/chronix/pb"
	"github.com/golang/protobuf/proto"
)

const almostEqualsOffsetMS = 10

// decode decodes a serialized stream of points.
func decode(compressed []byte, tsStart, tsEnd, from, to int64) ([]Point, error) {
	if from == -1 || to == -1 {
		return nil, fmt.Errorf("'from' or 'to' have to be >= 0")
	}

	// If to is left of the time series, we have no points to return.
	if to < tsStart {
		return nil, nil
	}
	// If from is greater to, we have nothing to return.
	if from > to {
		return nil, nil
	}
	// If from is right of the time series we have nothing to return.
	if from > tsEnd {
		return nil, nil
	}

	r, err := gzip.NewReader(bytes.NewBuffer(compressed))
	if err != nil {
		return nil, fmt.Errorf("error creating gzip reader: %v", err)
	}
	buf, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("error decompressing points: %v", err)
	}

	var pbPoints pb.Points
	if err = proto.Unmarshal(buf, &pbPoints); err != nil {
		return nil, fmt.Errorf("error unmarshalling points: %v", err)
	}

	lastOffset := int64(almostEqualsOffsetMS)
	calculatedPointDate := tsStart

	points := make([]Point, 0, len(pbPoints.P))

	for i, p := range pbPoints.P {
		if i > 0 {
			offset := p.GetT()
			if offset != 0 {
				lastOffset = offset
			}
			calculatedPointDate += lastOffset
		}

		// Only add the point if it is within the selected range.
		if calculatedPointDate >= from && calculatedPointDate <= to {
			points = append(points, Point{
				Timestamp: calculatedPointDate,
				Value:     p.GetV(),
			})
		}
	}
	return points, nil
}

// encode takes a series of points and encodes them.
func encode(points []Point) ([]byte, error) {
	var prevDate int64
	var prevOffset int64

	var timesSinceLastOffset int
	var lastStoredDate int64

	var pbPoints pb.Points
	pbPoints.P = make([]*pb.Point, 0, len(points))
	for _, p := range points {
		var offset int64
		if prevDate == 0 {
			offset = 0
			// Set lastStoredDate to the value of the first timestamp.
			lastStoredDate = p.Timestamp
		} else {
			offset = p.Timestamp - prevDate
		}

		// Semantic Compression.
		var pbPoint pb.Point
		if almostEqualsOffsetMS == -1 {
			pbPoint.V = proto.Float64(p.Value)
		} else {
			if almostEquals(prevOffset, offset) && noDrift(p.Timestamp, lastStoredDate, timesSinceLastOffset) {
				pbPoint.V = proto.Float64(p.Value)
				timesSinceLastOffset++
			} else {
				pbPoint.T = proto.Int64(offset)
				pbPoint.V = proto.Float64(p.Value)
				// Reset the offset counter.
				timesSinceLastOffset = 1
				lastStoredDate = p.Timestamp
			}

			// Set current as former previous date.
			prevOffset = offset
			prevDate = p.Timestamp
		}

		pbPoints.P = append(pbPoints.P, &pbPoint)
	}
	buf, err := proto.Marshal(&pbPoints)
	if err != nil {
		return nil, fmt.Errorf("error marshalling points: %v", err)
	}
	compressed := &bytes.Buffer{}
	w := gzip.NewWriter(compressed)
	if _, err = w.Write(buf); err != nil {
		return nil, fmt.Errorf("error compressing points: %v", err)
	}
	if err := w.Close(); err != nil {
		return nil, fmt.Errorf("error closing gzip writer: %v", err)
	}
	return compressed.Bytes(), nil
}

func noDrift(timestamp int64, lastStoredDate int64, timesSinceLastOffset int) bool {
	calculatedMaxOffset := almostEqualsOffsetMS * timesSinceLastOffset
	drift := lastStoredDate + int64(calculatedMaxOffset) - timestamp

	return drift <= almostEqualsOffsetMS/2
}

func almostEquals(previousOffset int64, offset int64) bool {
	diff := math.Abs(float64(offset - previousOffset))
	return diff <= almostEqualsOffsetMS
}
