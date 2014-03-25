package dataframe

import (
	"time"

	"code.google.com/p/goprotobuf/proto"
)

// NewRequestTag builds a source tag (RequestSource_Tag) from field and
// value strings.
func NewRequestTag(field, value string) *RequestSource_Tag {
	tagField := field
	tagValue := value
	tag := new(RequestSource_Tag)
	tag.Field = &tagField
	tag.Value = &tagValue
	return tag
}

func BuildRequestTags(tags map[string]string) []*RequestSource_Tag {
	ts := make([]*RequestSource_Tag, 0)
	for k, v := range tags {
		t := NewRequestTag(k, v)
		ts = append(ts, t)
	}
	return ts
}

func NewRequest(tags []*RequestSource_Tag, start, end time.Time) *RequestSource {
	r := new(RequestSource)
	r.Source = tags
	alpha := uint64(start.UnixNano())
	omega := uint64(end.UnixNano())
	r.Alpha = &alpha
	r.Omega = &omega
	return r
}

// Marshal returns the byte representation of the given source request.
func (r *RequestSource) Marshal() ([]byte, error) {
	b, err := proto.Marshal(r)
	return b, err
}
