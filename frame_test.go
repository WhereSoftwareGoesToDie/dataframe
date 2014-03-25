package dataframe

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

const (
	frameValue = 3.8064393162297505
)

func TestDataFrameMarshallingConsistency(t *testing.T) {
	frame := GenTestDataFrame()
	bytes, err := MarshalDataFrame(frame)
	t.Log(bytes)
	if err != nil {
		t.Errorf("Error marshalling test frame: %v", err)
	}
	newFrame, err := UnmarshalFrame(bytes)
	if err != nil {
		t.Errorf("Error unmarshalling test frame: %v", err)
	}
	for idx, tag := range newFrame.Source {
		originalTag := frame.Source[idx]
		if *tag.Field != *originalTag.Field {
			t.Errorf("Tag mismatch: field %v initially, %v after unmarshalling", *originalTag.Field, *tag.Field)
		}
		if *tag.Value != *originalTag.Value {
			t.Errorf("Tag mismatch: value %v initially, %v after unmarshalling", *originalTag.Value, *tag.Value)
		}
	}
	if *newFrame.Payload != DataFrame_REAL {
		t.Errorf("Expected payload REAL, got %v", *newFrame.Payload)
	}
	if *newFrame.ValueMeasurement != *frame.ValueMeasurement {
		t.Errorf("Value mismatch: %v initially, %v after unmarshalling", *frame.ValueMeasurement, *newFrame.ValueMeasurement)
	}
}

func openDataFrameTestFile(t *testing.T) io.Reader {
	stream, err := os.Open("testdata/test_frame.pb")
	if err != nil {
		t.Errorf("Could not open testdata: %v", err)
	}
	return stream
}

func TestUnmarshalFrame(t *testing.T) {
	stream := openDataFrameTestFile(t)
	packet, err := ioutil.ReadAll(stream)
	if err != nil {
		t.Errorf("Error reading DataFrame file: %v", err)
	}
	frame, err := UnmarshalFrame(packet)
	if err != nil {
		t.Errorf("Got error from UnmarshalFrame: %v", err)
	}
	if *frame.ValueMeasurement != frameValue {
		t.Errorf("Value mismatch: expected %v, got %v", frameValue, *frame.ValueMeasurement)
	}
}

// Generate two different frames, ensure they're still different, bring
// them closer together, and check they end up identical.
func TestFrameIdentity(t *testing.T) {
	a := GenTestDataFrame()
	// Ensure next packet has a different timestamp.
	time.Sleep(time.Microsecond)
	b := GenTestDataFrame()
	if a.Identical(*b) {
		t.Errorf("Tested two sequentially-generated frames as identical: \n\n%v\n%v", a, b)
	}
	oldMeasurement := *a.ValueMeasurement
	a.ValueMeasurement = b.ValueMeasurement
	if a.Identical(*b) {
		t.Errorf("Tested two sequentially-generated frames as "+
			"identical (timestamps differ): \n\n%v\n%v", a, b)
	}
	a.ValueMeasurement = &oldMeasurement
	a.Timestamp = b.Timestamp
	if a.Identical(*b) {
		t.Errorf("Tested two sequentially-generated frames as "+
			"identical (values differ): \n\n%v\n%v", a, b)
	}
	a.ValueMeasurement = b.ValueMeasurement
	if !a.Identical(*b) {
		t.Errorf("Tested two converged frames as different: "+
			"\n\n%v\n%v", a, b)
	}
}
