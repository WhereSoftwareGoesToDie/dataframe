package dataframe

import (
	"testing"
)

func benchmarkDataFrameProtobuf(b *testing.B) {
	frames := make([]*DataFrame, b.N)
	frame := GenTestDataFrame()
	for i, _ := range frames {
		frames[i] = frame
	}
	burst := BuildDataBurst(frames)
	_, _ = MarshalDataBurst(burst)
}

func BenchmarkProtobuf10(b *testing.B) { benchmarkDataFrameProtobuf(b) }
func BenchmarkProtobuf1000(b *testing.B) { benchmarkDataFrameProtobuf(b) }
func BenchmarkProtobuf100000(b *testing.B) { benchmarkDataFrameProtobuf(b) }
