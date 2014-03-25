package framestore

import (
	"github.com/anchor/dataframe"
)

// Each storage backend will have a Writer implementing this interface.
type DataFrameWriter interface {
	WriteFrame(frame *dataframe.DataFrame) error
	Shutdown() error
}

type DataBurstWriter interface {
	WriteBurst(burst *dataframe.DataBurst) error
	Shutdown() error
}
