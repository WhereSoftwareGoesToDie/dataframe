package dataframe

import (
	"code.google.com/p/goprotobuf/proto"
)

// BuildDataBurst returns a DataBurst from a slice of DataFrames.
func BuildDataBurst(frames []*DataFrame) *DataBurst {
	burst := new(DataBurst)
	burst.Frames = frames
	return burst
}

// MarshalDataBurst takes a DataBurst pointer and returns its
// representation as a byteslice.
func MarshalDataBurst(burst *DataBurst) ([]byte, error) {
	marshalledBurst, err := proto.Marshal(burst)
	return marshalledBurst, err
}

// Unmarshal one DataBurst from one packet. Packet is
// assumed to be complete and without trailing bytes. Return a
// DataBurst*.
func UnmarshalDataBurst(packet []byte) (*DataBurst, error) {
	burst := new(DataBurst)
	err := proto.Unmarshal(packet, burst)
	return burst, err
}
