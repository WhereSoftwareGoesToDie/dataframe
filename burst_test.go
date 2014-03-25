package dataframe

import (
	"io"
	"io/ioutil"
	"os"
	"testing"
)

const (
	firstDataBurstValue = 7.040200607505408
	dataBurstLength     = 100
)

func TestDataBurstMarshallingConsistency(t *testing.T) {
	frames := make([]*DataFrame, 1)
	frames[0] = GenTestDataFrame()
	burst := BuildDataBurst(frames)
	bytes, err := MarshalDataBurst(burst)
	t.Log(bytes)
	if err != nil {
		t.Errorf("Error marshalling test burst: %v", err)
	}
	newBurst, err := UnmarshalDataBurst(bytes)
	if err != nil {
		t.Errorf("Error unmarshalling test burst: %v", err)
	}
	newFrame := newBurst.Frames[0]
	for idx, tag := range newFrame.Source {
		originalTag := frames[0].Source[idx]
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
	if *newFrame.ValueMeasurement != *frames[0].ValueMeasurement {
		t.Errorf("Value mismatch: %v initially, %v after unmarshalling", *frames[0].ValueMeasurement, *newFrame.ValueMeasurement)
	}
}

func openDataBurstTestFile(t *testing.T) io.Reader {
	stream, err := os.Open("testdata/test_burst.pb")
	if err != nil {
		t.Errorf("Could not open testdata: %v", err)
	}
	return stream
}

func TestUnmarshalDataBurst(t *testing.T) {
	stream := openDataBurstTestFile(t)
	packet, err := ioutil.ReadAll(stream)
	if err != nil {
		t.Errorf("Error reading DataBurst file: %v", err)
	}
	burst, err := UnmarshalDataBurst(packet)
	if err != nil {
		t.Errorf("Got error from UnmarshalDataBurst: %v", err)
	}
	if len(burst.Frames) != 100 {
		t.Errorf("Frame count mismatch: expected %v, got %v", dataBurstLength, len(burst.Frames))
	}
	if *burst.Frames[0].ValueMeasurement != firstDataBurstValue {
		t.Errorf("Value mismatch: expected %v, got %v", firstDataBurstValue, burst.Frames[0].ValueMeasurement)
	}
}
