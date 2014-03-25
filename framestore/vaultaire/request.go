package vaultaire

import (
	"time"
	"syscall"

	"github.com/anchor/dataframe"
	zmq "github.com/pebbe/zmq4"
)

type ReaderContext struct {
	sock *zmq.Socket
}

// NewReaderContext initializes the ZMQ context and connects to the
// readerd at the supplied endpoint.
func NewReaderContext(endpoint string) (*ReaderContext, error) {
	var err error
	c := new(ReaderContext)
	c.sock, err = zmq.NewSocket(zmq.DEALER)
	if err != nil {
		return nil, err
	}
	err = c.sock.Connect(endpoint)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// RequestFrames makes a request to readerd for the data for a given
// source between start and end, returning an array of DataBursts.
func (c *ReaderContext) RequestFrames(origin string, source map[string]string, start, end time.Time) ([]*dataframe.DataBurst, error) {
	tags := dataframe.BuildRequestTags(source)
	req := dataframe.NewRequest(tags, start, end)
	reqBytes, err := req.Marshal()
	if err != nil {
		return nil, err
	}
	_, err = c.sock.SendMessage(origin, reqBytes)
	if err != nil {
		return nil, err
	}
	response := make([][]byte, 0)
	for {
		msg, err := c.sock.RecvBytes(0)
		if err != nil && err != syscall.EAGAIN && err != syscall.EINTR {
			return nil, err
		}
		// Check if we're done reading
		if len(msg) == 0 {
			break
		}
		response = append(response, msg)
	}
	bursts := make([]*dataframe.DataBurst, len(response))
	for i, msg := range response {
		bursts[i], err = dataframe.UnmarshalDataBurst(msg)
		if err != nil {
			return nil, err
		}
	}
	return bursts, nil
}
