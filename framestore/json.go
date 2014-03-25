package framestore

import (
	"encoding/json"
	"io"

	"github.com/anchor/dataframe"
)

// Implements dataframe.DataFrameWriter interface.
// This backend will append to a file, creating it if it does not exist.
type JSONWriter struct {
	stream   io.Writer
	filename string
}

func NewJSONWriter(stream io.Writer) (*JSONWriter, error) {
	writer := new(JSONWriter)
	writer.stream = stream
	return writer, nil
}

func (w JSONWriter) WriteFrame(frame *dataframe.DataFrame) error {
	jsonFrame := frame.ToJSON()
	marshalledFrame, err := json.Marshal(jsonFrame)
	if err != nil {
		return err
	}
	_, err = w.stream.Write(marshalledFrame)
	return err
}
