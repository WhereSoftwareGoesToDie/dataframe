package framestore

import (
	"github.com/anchor/dataframe"
)

// DataFrameReader reads frames sequentially from a store suited to
// sequential reads, like a file. See also DataFrameGetter.
type DataFrameReader interface {
	// Reads one frame from the backing store and returns it.
	ReadFrame() (*dataframe.DataFrame, error)
}
