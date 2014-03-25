package dataframe

import (
	"sort"
)

type orderedBurst []*DataFrame

func (b orderedBurst) Len() int {
	return len(b)
}

func (b orderedBurst) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

type burstByTimestamp struct { orderedBurst }

func (b burstByTimestamp) Less(i, j int) bool {
	return *b.orderedBurst[i].Timestamp < *b.orderedBurst[j].Timestamp
}

// SortByTimestamp sorts the frames in a DataBurst by timestamp
// (in-place). Possibly unstable. 
func (b *DataBurst) SortByTimestamp() {
	sort.Sort(burstByTimestamp{b.Frames})
}
