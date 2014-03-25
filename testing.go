package dataframe

import (
	"math/rand"
	"time"
)

// Return a struct of key/value pairs in the form of a slice of pointers
// to DataFrame_Tags.
func genTestDataSource() []*DataFrame_Tag {
	source := make([]*DataFrame_Tag, 0)
	source = append(source, NewDataFrameTag("origin", TestDataSourcePrefix))
	source = append(source, NewDataFrameTag("hostname", "hut4"))
	source = append(source, NewDataFrameTag("service_name", "bombe"))
	source = append(source, NewDataFrameTag("metric", "runtime"))
	return source
}

// Generate a test frame with a random (REAL) value between zero and
// ten, and the current time.
func GenTestDataFrame() *DataFrame {
	rand.Seed(time.Now().UTC().UnixNano())
	frame := new(DataFrame)
	dataSource := genTestDataSource()
	timestamp := uint64(time.Now().UnixNano())
	payload := DataFrame_REAL
	valueMeasurement := rand.Float64() * 10.0
	frame.Source = dataSource
	frame.Timestamp = &timestamp
	frame.Payload = &payload
	frame.ValueMeasurement = &valueMeasurement
	return frame
}
