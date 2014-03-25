package vaultaire

import (
	"fmt"
	"github.com/anchor/dataframe"
	"github.com/anchor/gomarquise"
)

// VaultaireWriter writes DataFrames to vaultaire (via libmarquise). It
// implements framestore.DataFrameWriter.
type VaultaireWriter struct {
	context *gomarquise.MarquiseContext
}

// zmqBroker is a string taking the form of a ZeroMQ URI. batchPeriod
// is the interval at which the worker thread will poll/empty the queue
// of messages.
func NewVaultaireWriter(zmqBroker string, batchPeriod float64, origin, telemetry string, debug bool) (VaultaireWriter, error) {
	var err error
	writer := new(VaultaireWriter)
	writer.context, err = gomarquise.Dial(zmqBroker, batchPeriod, origin, telemetry, debug)
	return *writer, err
}

// Shutdown ensures that relevant zeromq sockets get closed - as
// libmarquise writes are nonblocking, this must be called in order to
// ensure that previous writes are safe.
func (w VaultaireWriter) Shutdown() error {
	w.context.Shutdown()
	return nil
}

// WriteFrame takes a single DataFrame pointer and writes it to
// libmarquise, which will then write it to Vaultaire.
//
// There is no batch-write support in the framestore package as writes
// are non-blocking and batched by libmarquise.
func (w VaultaireWriter) WriteFrame(frame *dataframe.DataFrame) error {
	sourceMap := make(map[string]string, 0)
	for _, tag := range frame.Source {
		sourceMap[*tag.Field] = *tag.Value
	}
	switch {
	case *frame.Payload == dataframe.DataFrame_EMPTY:
		return w.context.WriteCounter(sourceMap, *frame.Timestamp)
	case *frame.Payload == dataframe.DataFrame_NUMBER:
		return w.context.WriteInt(sourceMap, *frame.ValueNumeric, *frame.Timestamp)
	case *frame.Payload == dataframe.DataFrame_REAL:
		return w.context.WriteReal(sourceMap, *frame.ValueMeasurement, *frame.Timestamp)
	case *frame.Payload == dataframe.DataFrame_BINARY:
		return w.context.WriteBinary(sourceMap, frame.ValueBlob, *frame.Timestamp)
	case *frame.Payload == dataframe.DataFrame_TEXT:
		return w.context.WriteText(sourceMap, *frame.ValueTextual, *frame.Timestamp)
	}
	return fmt.Errorf("Invalid payload type %v", *frame.Payload)
}
