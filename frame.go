package dataframe

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"io"
	"log"
)

const (
	TestDataSourcePrefix = "bletchley/testframe"
)

// NewDataFrameTag builds a source tag (DataFrameTag) from field and
// value strings.
func NewDataFrameTag(field, value string) *DataFrame_Tag {
	tagField := field
	tagValue := value
	tag := new(DataFrame_Tag)
	tag.Field = &tagField
	tag.Value = &tagValue
	return tag
}

// Identical takes a DataFrame and does a deep compare; returns true if
// all fields in both frames match and false otherwise.
func (f DataFrame) Identical(g DataFrame) bool {
	if *f.Payload != *g.Payload {
		return false
	}
	// Missing optional values are null pointers, not just pointers
	// to zero values, so we need to check the type before comparing
	// the value.
	switch {
	case *f.Payload == DataFrame_NUMBER:
		if *f.ValueNumeric != *g.ValueNumeric {
			return false
		}
	case *f.Payload == DataFrame_REAL:
		if *f.ValueMeasurement != *g.ValueMeasurement {
			return false
		}
	case *f.Payload == DataFrame_TEXT:
		if *f.ValueTextual != *g.ValueTextual {
			return false
		}
	case *f.Payload == DataFrame_BINARY:
		if bytes.Compare(f.ValueBlob, g.ValueBlob) != 0 {
			return false
		}
	}
	if *f.Timestamp != *g.Timestamp {
		return false
	}
	if bytes.Compare(f.Origin, g.Origin) != 0 {
		return false
	}
	if len(f.Source) != len(g.Source) {
		return false
	}
	for i, _ := range f.Source {
		if *f.Source[i].Field != *g.Source[i].Field {
			return false
		}
		if *f.Source[i].Value != *g.Source[i].Value {
			return false
		}
	}
	return true
}

// Return a byteslice ready to write to file/socket/whatever from a
// DataFrame. This function performs no framing; if you need framing,
// use a DataBurst instead.
func MarshalDataFrame(frame *DataFrame) ([]byte, error) {
	marshalledFrame, err := proto.Marshal(frame)
	msgBuffer := new(bytes.Buffer)
	if err != nil {
		empty := make([]byte, 0)
		return empty, err
	}
	_, err = msgBuffer.Write(marshalledFrame)
	return msgBuffer.Bytes(), err
}

// Unmarshal one frame from one packet (as a byteslice). The packet is
// assumed to be complete and without trailing bytes. Return a
// DataFrame.
func UnmarshalFrame(packet []byte) (*DataFrame, error) {
	frame := new(DataFrame)
	err := proto.Unmarshal(packet, frame)
	return frame, err
}

// Unmarshal one frame with an intact byte-count header (you shouldn't
// need to use this, it's just here for testing purposes).
func unmarshalPacketWithSizeHeader(packet []byte) (*DataFrame, error) {
	return UnmarshalFrame(packet[4:])
}

// Given a stream, return the size of the first packet in that stream.
func getPacketSize(stream io.Reader) (int, error) {
	var packetSize uint32
	var err error
	err = binary.Read(stream, binary.BigEndian, &packetSize)
	if err == io.EOF {
		return 0, io.EOF
	} else if err != nil {
		log.Println(err)
		return 0, err
	}
	return int(packetSize), nil
}
