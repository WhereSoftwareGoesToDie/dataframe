package dataframe

import (
	"encoding/base64"
	"fmt"
	"strconv"
)

// JSON-decorated equivalent of DataFrame_Tag
type JSONFrameTag struct {
	Field string `json:"tag_field"`
	Value string `json:"tag_value"`
}

// JSON-annotated representation of a DataFrame, for storage backends
// that lean that way (riak, elasticsearch).
type JSONFrame struct {
	DataSource []JSONFrameTag `json:"metric"`
	Value      string         `json:"value"`
	Timestamp  uint64         `json:"timestamp"`
	ValueType  string         `json:"value_type"`
}

// Return the JSON-decorated-struct representation of a slice of
// DataFrame_Tags
func DataFrameTagsToJSON(tags []*DataFrame_Tag) []JSONFrameTag {
	tagSlice := make([]JSONFrameTag, len(tags))
	for i, tagPtr := range tags {
		tagSlice[i].Field = *tagPtr.Field
		tagSlice[i].Value = *tagPtr.Value
	}
	return tagSlice
}

// Build a JSONFrame object from a DataFrame.
//
// We shouldn't see many binary blobs in Bletchley (hopefully), so we
// just base64encode 'em and hope for the best.
//
// FIXME: revisit this later and check if ^ is actually appropriate.
func (frame DataFrame) ToJSON() *JSONFrame {
	riakFrame := new(JSONFrame)
	riakFrame.DataSource = DataFrameTagsToJSON(frame.Source)
	switch {
	case *frame.Payload == DataFrame_NUMBER:
		riakFrame.Value = strconv.FormatInt(*frame.ValueNumeric, 10)
		riakFrame.ValueType = "NUMBER"
	case *frame.Payload == DataFrame_REAL:
		// Render the float in standard d.d format, losslessly.
		riakFrame.Value = strconv.FormatFloat(*frame.ValueMeasurement, 'f', -1, 64)
		riakFrame.ValueType = "REAL"
	case *frame.Payload == DataFrame_TEXT:
		riakFrame.Value = *frame.ValueTextual
		riakFrame.ValueType = "TEXTUAL"
	case *frame.Payload == DataFrame_BINARY:
		riakFrame.Value = base64.StdEncoding.EncodeToString(frame.ValueBlob)
		riakFrame.ValueType = "BINARY"
	case *frame.Payload == DataFrame_EMPTY:
		riakFrame.Value = "EMPTY"
		riakFrame.ValueType = "EMPTY"
	}
	riakFrame.Timestamp = *frame.Timestamp
	return riakFrame
}

// GetKey returns a unique string representation (underscore-separated
// fields) of the DataSource of a JSONFrame.
func (r JSONFrame) GetKey() string {
	var key string
	for _, tag := range r.DataSource {
		key += fmt.Sprintf("%v_%v_", tag.Field, tag.Value)
	}
	return fmt.Sprintf("%v_%v", key, r.Timestamp)
}
