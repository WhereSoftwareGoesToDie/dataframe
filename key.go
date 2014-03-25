package dataframe

import (
	"fmt"
)

// SourceKey is a type for consistent serialization of Sources to keys
// suitable for use in key-value stores.
type SourceKey string

func buildSourceKey(source []*DataFrame_Tag) SourceKey {
	var k SourceKey
	for _, tag := range source {
		k += SourceKey(fmt.Sprintf("%v_%v_", tag.Field, tag.Value))
	}
	return k
}

func (d *DataFrame) GetSourceKey() SourceKey {
	return buildSourceKey(d.Source)
}
