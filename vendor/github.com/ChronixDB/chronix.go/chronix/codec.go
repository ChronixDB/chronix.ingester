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

	lastDelta := int64(pbPoints.GetDdc())
	calculatedPointDate := tsStart

	points := make([]Point, 0, len(pbPoints.P))

	for i, p := range pbPoints.P {
		// Decode the time.
		if i > 0 {
			lastDelta = getTimestamp(p, lastDelta)
			calculatedPointDate += int64(lastDelta)
		}

		// Only add the point if it is within the selected range.
		if calculatedPointDate >= from && calculatedPointDate <= to {
			var value float64
			if p.VIndex != nil {
				value = pbPoints.P[p.GetVIndex()].GetV()
			} else {
				value = p.GetV()
			}
			points = append(points, Point{
				Timestamp: calculatedPointDate,
				Value:     value,
			})
		}
	}
	return points, nil
}

// encode takes a series of points and encodes them.
func encode(points []Point, ddcThreshold uint32) ([]byte, error) {
	var (
		prevDate  int64
		prevDelta int64
		prevDrift int64

		startDate      int64
		lastStoredDate int64

		delta           int64
		lastStoredDelta int64

		timesSinceLastDelta int32
	)

	valueIndex := map[float64]uint32{}

	var pbPoints pb.Points
	pbPoints.P = make([]*pb.Point, 0, len(points))

	var index uint32
	for i, p := range points {
		var pbPoint pb.Point
		currentTimestamp := p.Timestamp
		// Add value or index, if the value already exists.
		setValueOrRefIndexOnPoint(valueIndex, index, p.Value, &pbPoint)
		if prevDate == 0 {
			// Set lastStoredDate to the value of the first timestamp.
			lastStoredDate = currentTimestamp
			startDate = currentTimestamp
		} else {
			delta = currentTimestamp - prevDate
		}

		// Last point.
		if i == len(points)-1 {
			handleLastPoint(ddcThreshold, startDate, &pbPoint, &pbPoints, currentTimestamp)
			break
		}

		// We have a normal point.
		isAlmostEqual := almostEquals(prevDelta, delta, ddcThreshold)
		var drift int64

		// The deltas of the timestamps are almost equal (delta < ddcThreshold).
		if isAlmostEqual {
			// Calculate the drift to the actual timestamp.
			drift = calculateDrift(currentTimestamp, lastStoredDate, timesSinceLastDelta, lastStoredDelta)
		}

		if isAlmostEqual && noDrift(drift, ddcThreshold, timesSinceLastDelta) && drift >= 0 {
			timesSinceLastDelta++
		} else {
			timestamp := delta
			// If the previous offset was not stored, correct the following delta using the calculated drift.
			if timesSinceLastDelta > 0 && delta > prevDrift {
				timestamp = delta - prevDrift
				setBPTimestamp(&pbPoint, timestamp)
			} else {
				setTimestamp(&pbPoint, timestamp)
			}

			// Reset the offset counter.
			timesSinceLastDelta = 0
			lastStoredDate = p.Timestamp
			lastStoredDelta = timestamp
		}
		pbPoints.P = append(pbPoints.P, &pbPoint)

		// Set current as former previous date.
		prevDrift = drift
		prevDelta = delta
		prevDate = currentTimestamp

		index++
	}

	// Set the ddc value.
	pbPoints.Ddc = &ddcThreshold

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

func getTimestamp(p *pb.Point, lastOffset int64) int64 {
	// Normal delta.
	if p.Tint != nil || p.Tlong != nil {
		return int64(p.GetTint()) + int64(p.GetTlong())
	}
	if p.TintBP != nil || p.TlongBP != nil {
		return int64(p.GetTintBP()) + int64(p.GetTlongBP())
	}
	return lastOffset
}

func handleLastPoint(ddcThreshold uint32, startDate int64, point *pb.Point, points *pb.Points, currentTimestamp int64) {
	calcPoint := calculateTimestamp(startDate, points.P, ddcThreshold)

	// Calculate offset.
	deltaToLastTimestamp := currentTimestamp - calcPoint

	// Everything okay?
	if deltaToLastTimestamp >= 0 {
		setTimestamp(point, deltaToLastTimestamp)
		points.P = append(points.P, point)
	} else {
		// We have to rearrange the points as we are already behind the actual end timestamp.
		rearrangePoints(startDate, currentTimestamp, deltaToLastTimestamp, ddcThreshold, points, point)
	}
}

func setValueOrRefIndexOnPoint(index map[float64]uint32, currentPointIndex uint32, value float64, point *pb.Point) {
	// Build value index.
	if i, exists := index[value]; exists {
		point.VIndex = proto.Uint32(i)
	} else {
		index[value] = currentPointIndex
		point.V = proto.Float64(value)
	}
}

func setTimestamp(point *pb.Point, timestampDelta int64) {
	if safeLongToUInt(timestampDelta) {
		point.Tint = proto.Uint32(uint32(timestampDelta))
	} else {
		point.Tlong = proto.Uint64(uint64(timestampDelta))
	}
}

func setBPTimestamp(point *pb.Point, timestampDelta int64) {
	if safeLongToUInt(timestampDelta) {
		point.TintBP = proto.Uint32(uint32(timestampDelta))
	} else {
		point.TlongBP = proto.Uint64(uint64(timestampDelta))
	}
}

func rearrangePoints(startDate int64, currentTimestamp int64, deltaToEndTimestamp int64, ddcThreshold uint32, points *pb.Points, point *pb.Point) {
	// Break the offset down on all points.
	avgPerDelta := int64(math.Ceil(float64(deltaToEndTimestamp*-1+int64(ddcThreshold)) / float64(len(points.P)-1)))

	for i := 1; i < len(points.P); i++ {
		mod := points.P[i]
		t := getT(mod)

		// Check if we can correct the deltas.
		if deltaToEndTimestamp < 0 {
			var newOffset int64

			if deltaToEndTimestamp+avgPerDelta > 0 {
				avgPerDelta = deltaToEndTimestamp * -1
			}

			// If we have a t value.
			if t > avgPerDelta {
				newOffset = t - avgPerDelta
				modPoint := proto.Clone(mod).(*pb.Point)
				setT(modPoint, newOffset)
				mod = modPoint
			}

		}
		points.P[i] = mod
	}

	// Done.
	arrangedPoint := calculateTimestamp(startDate, points.P, ddcThreshold)

	storedOffsetToEnd := currentTimestamp - arrangedPoint
	if storedOffsetToEnd < 0 {
		panic("stored offset is negative")
	}

	setBPTimestamp(point, storedOffsetToEnd)

	points.P = append(points.P, point)
}

func setT(point *pb.Point, delta int64) {
	if safeLongToUInt(delta) {
		if point.TintBP != nil {
			point.TintBP = proto.Uint32(uint32(delta))
		}
		if point.Tint != nil {
			point.Tint = proto.Uint32(uint32(delta))
		}
	} else {
		if point.TlongBP != nil {
			point.TlongBP = proto.Uint64(uint64(delta))
		}
		if point.Tlong != nil {
			point.Tlong = proto.Uint64(uint64(delta))
		}
	}
}

func getT(point *pb.Point) int64 {
	// Only one is set, others are zero.
	return int64(point.GetTlongBP()+point.GetTlong()) + int64(point.GetTint()+point.GetTintBP())
}

func safeLongToUInt(value int64) bool {
	return !(value < 0 || value > math.MaxInt32)
}

func calculateTimestamp(startDate int64, points []*pb.Point, ddcThreshold uint32) int64 {
	lastDelta := int64(ddcThreshold)
	calculatedPointDate := startDate

	for i := 1; i < len(points); i++ {
		p := points[i]
		lastDelta = getTimestamp(p, lastDelta)
		calculatedPointDate += lastDelta
	}
	return calculatedPointDate
}

func noDrift(drift int64, ddcThreshold uint32, timesSinceLastStoredDelta int32) bool {
	return timesSinceLastStoredDelta == 0 || drift == 0 || drift < int64(ddcThreshold/2)
}

func calculateDrift(timestamp int64, lastStoredDate int64, timesSinceLastDelta int32, lastStoredDelta int64) int64 {
	calculatedMaxOffset := lastStoredDelta * (int64(timesSinceLastDelta) + 1)
	return lastStoredDate + calculatedMaxOffset - timestamp
}

func almostEquals(prevOffset int64, offset int64, almostEquals uint32) bool {
	diff := math.Abs(float64(offset - prevOffset))
	return diff <= float64(almostEquals)
}
