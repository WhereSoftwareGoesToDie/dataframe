// Dataframe readers/writers for files.

package framestore

import (
	"io/ioutil"
	"log"
	"os"

	"github.com/anchor/dataframe"
)

// This backend will append to a file, creating it if it does not exist.
type FileWriter struct {
	stream   *os.File
	filename string
}

func NewFileWriter(filename string) (*FileWriter, error) {
	var err error
	writer := new(FileWriter)
	writer.filename = filename
	writer.stream, err = os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		log.Println("Could not create file ", filename, ": ", err)
		return nil, err
	}
	return writer, nil
}

// WriteFrame will write a single frame to the output file. Note that
// this function isn't very useful with regular files if you're writing
// more than one DataFrame, due to the lack of framing.
func (w FileWriter) WriteFrame(frame *dataframe.DataFrame) error {
	marshalledMessage, err := dataframe.MarshalDataFrame(frame)
	if err != nil {
		return err
	}
	_, err = w.stream.Write(marshalledMessage)
	return err
}

// WriteBurst writes a single DataBurst to the output file. As above,
// don't try to put more than one DataBurst into one regular file.
func (w FileWriter) WriteBurst(burst *dataframe.DataBurst) error {
	marshalledMessage, err := dataframe.MarshalDataBurst(burst)
	if err != nil {
		return err
	}
	_, err = w.stream.Write(marshalledMessage)
	return err
}

func (w FileWriter) Shutdown() error {
	return nil
}

type FileReader struct {
	stream   *os.File
	filename string
}

func NewFileReader(filename string) (*FileReader, error) {
	var err error
	reader := new(FileReader)
	reader.filename = filename
	reader.stream, err = os.Open(filename)
	if err != nil {
		log.Printf("Could not open file %v for reading.", filename)
		return nil, err
	}
	return reader, nil
}

func (r FileReader) ReadFrame() (*dataframe.DataFrame, error) {
	packet, err := ioutil.ReadAll(r.stream)
	if err != nil {
		return nil, err
	}
	frame, err := dataframe.UnmarshalFrame(packet)
	return frame, err
}
