// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"testing"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

// The examples of spec 3.0.0, 6.4.17, except that FloatArray and DoubleArray are little-endian (see arrayElems), and
// that -23 is 0xE9: the spec's Int8Array example gives 0xEF, which is -17.
var arrayExamples = []struct {
	dt    protobuf.DataType
	json  string
	bytes []byte
	elems []any
}{
	{protobuf.DataType_Int8Array, "[-23, 123]", []byte{0xE9, 0x7B}, []any{-23.0, 123.0}},
	{protobuf.DataType_Int16Array, "[-30000, 30000]", []byte{0xD0, 0x8A, 0x30, 0x75}, []any{-30000.0, 30000.0}},
	{protobuf.DataType_Int32Array, "[-1, 315338746]", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFA, 0xAF, 0xCB, 0x12}, []any{-1.0, 315338746.0}},
	{protobuf.DataType_Int64Array, "[-4270929666821191986, -3601064768563266876]",
		[]byte{0xCE, 0x06, 0x72, 0xAC, 0x18, 0x9C, 0xBA, 0xC4, 0xC4, 0xBA, 0x9C, 0x18, 0xAC, 0x72, 0x06, 0xCE},
		[]any{-4270929666821191986.0, -3601064768563266876.0}},
	{protobuf.DataType_UInt8Array, "[23, 250]", []byte{0x17, 0xFA}, []any{23.0, 250.0}},
	{protobuf.DataType_UInt16Array, "[30, 52360]", []byte{0x1E, 0x00, 0x88, 0xCC}, []any{30.0, 52360.0}},
	{protobuf.DataType_UInt32Array, "[52, 3293969225]", []byte{0x34, 0x00, 0x00, 0x00, 0x49, 0xFB, 0x55, 0xC4}, []any{52.0, 3293969225.0}},
	{protobuf.DataType_UInt64Array, "[52, 16444743074749521625]",
		[]byte{0x34, 0, 0, 0, 0, 0, 0, 0, 0xD9, 0x9E, 0x02, 0xD1, 0xB2, 0x76, 0x37, 0xE4},
		[]any{52.0, 16444743074749521625.0}},
	{protobuf.DataType_FloatArray, "[1.5, -2.25]", []byte{0x00, 0x00, 0xC0, 0x3F, 0x00, 0x00, 0x10, 0xC0}, []any{1.5, -2.25}},
	{protobuf.DataType_DoubleArray, "[1.5]", []byte{0, 0, 0, 0, 0, 0, 0xF8, 0x3F}, []any{1.5}},
	{protobuf.DataType_BooleanArray, "[false, false, true, true, false, true, false, false, true, true, false, true]",
		[]byte{0x0C, 0x00, 0x00, 0x00, 0x34, 0xD0},
		[]any{false, false, true, true, false, true, false, false, true, true, false, true}},
	{protobuf.DataType_StringArray, `["ABC", "hello"]`, []byte{0x41, 0x42, 0x43, 0x00, 0x68, 0x65, 0x6C, 0x6C, 0x6F, 0x00}, []any{"ABC", "hello"}},
	{protobuf.DataType_StringArray, "[]", nil, []any{}},
}

func TestArrayExamples(t *testing.T) {
	for _, tt := range arrayExamples {
		t.Run(tt.dt.String()+" "+tt.json, func(t *testing.T) {
			b, err := encodeArray(tt.dt, tt.json)
			require.NoError(t, err)
			assert.Equal(t, tt.bytes, b)
			elems, ok := decodeArray(tt.dt, tt.bytes)
			require.True(t, ok)
			assert.Equal(t, tt.elems, elems)
		})
	}
}

func TestEncodeArrayFromSlice(t *testing.T) {
	b, err := encodeArray(protobuf.DataType_Int16Array, []int{-30000, 30000})
	require.NoError(t, err)
	assert.Equal(t, []byte{0xD0, 0x8A, 0x30, 0x75}, b)
	b, err = encodeArray(protobuf.DataType_FloatArray, []any{1.5, "-2.25"})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x00, 0x00, 0xC0, 0x3F, 0x00, 0x00, 0x10, 0xC0}, b)
	b, err = encodeArray(protobuf.DataType_BooleanArray, []bool{true})
	require.NoError(t, err)
	assert.Equal(t, []byte{0x01, 0, 0, 0, 0x80}, b)
}

func TestEncodeArrayRejects(t *testing.T) {
	tests := []struct {
		name  string
		dt    protobuf.DataType
		value any
	}{
		{"element just out of range", protobuf.DataType_Int8Array, "[127, 128]"},
		{"negative unsigned element", protobuf.DataType_UInt16Array, "[-1]"},
		{"fraction in an integer array", protobuf.DataType_Int32Array, "[1.5]"},
		{"float32 overflow", protobuf.DataType_FloatArray, "[3.5e38]"},
		{"not a JSON array", protobuf.DataType_Int8Array, "1"},
		{"trailing data", protobuf.DataType_Int8Array, "[1] [2]"},
		{"trailing bracket", protobuf.DataType_Int8Array, "[1]]"},
		{"trailing brace", protobuf.DataType_Int8Array, "[1]}"},
		{"JSON null", protobuf.DataType_Int16Array, "null"},
		{"JSON null for booleans", protobuf.DataType_BooleanArray, "null"},
		{"not a slice", protobuf.DataType_Int8Array, 1},
		{"number in a string array", protobuf.DataType_StringArray, "[1]"},
		{"NUL in a string element", protobuf.DataType_StringArray, []string{"a\x00b"}},
		{"number in a boolean array", protobuf.DataType_BooleanArray, "[1]"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := encodeArray(tt.dt, tt.value)
			assert.Error(t, err)
		})
	}
}

func TestDecodeArrayRejectsPartialElements(t *testing.T) {
	tests := []struct {
		name  string
		dt    protobuf.DataType
		bytes []byte
	}{
		{"half an Int16", protobuf.DataType_Int16Array, []byte{0x01, 0x02, 0x03}},
		{"Double cut short", protobuf.DataType_DoubleArray, []byte{0, 0, 0, 0}},
		{"no boolean count", protobuf.DataType_BooleanArray, []byte{0x01, 0x00}},
		{"fewer boolean bytes than the count", protobuf.DataType_BooleanArray, []byte{0x09, 0, 0, 0, 0xFF}},
		{"more boolean bytes than the count", protobuf.DataType_BooleanArray, []byte{0x08, 0, 0, 0, 0xFF, 0xFF}},
		// The count overflows an int on a 32-bit platform.
		{"boolean count beyond the bytes", protobuf.DataType_BooleanArray, []byte{0xFF, 0xFF, 0xFF, 0xFF}},
		{"unterminated string", protobuf.DataType_StringArray, []byte{0x41, 0x00, 0x42}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := decodeArray(tt.dt, tt.bytes)
			assert.False(t, ok)
		})
	}
}

func TestSetValueArray(t *testing.T) {
	m := &protobuf.Payload_Metric{}
	require.NoError(t, setValue(m, protobuf.DataType_UInt8Array, "[23, 250]"))
	assert.Equal(t, []byte{0x17, 0xFA}, m.GetBytesValue())

	m = &protobuf.Payload_Metric{}
	assert.Error(t, setValue(m, protobuf.DataType_UInt8Array, "[256]"))
	assert.Nil(t, m.GetValue(), "a rejected value must not be set")
}

func TestArrayReadingConvertsToEdgeXEvent(t *testing.T) {
	tests := []struct {
		dt        protobuf.DataType
		bytes     []byte
		valueType string
		value     any
	}{
		{protobuf.DataType_Int16Array, []byte{0xD0, 0x8A, 0x30, 0x75}, common.ValueTypeInt16Array, "[-30000, 30000]"},
		{protobuf.DataType_BooleanArray, []byte{0x02, 0, 0, 0, 0x40}, common.ValueTypeBoolArray, "[false, true]"},
		{protobuf.DataType_StringArray, []byte{0x61, 0x00}, common.ValueTypeStringArray, `["a"]`},
	}
	for _, tt := range tests {
		t.Run(tt.dt.String(), func(t *testing.T) {
			def := models.MetricDef{Name: "r", Datatype: uint32(tt.dt)}
			m := &protobuf.Payload_Metric{Timestamp: proto.Uint64(1), Value: &protobuf.Payload_Metric_BytesValue{BytesValue: tt.bytes}}
			reading, err := newReading(def, m, &protobuf.Payload{})
			require.NoError(t, err)
			assert.Equal(t, tt.valueType, reading.Type)
			event, err := xrtmodels.ToEdgeXV2EventDTO(xrtmodels.MultiResourcesResult{Device: "d", Readings: map[string]xrtmodels.Reading{"r": reading}})
			require.NoError(t, err)
			assert.Equal(t, tt.value, event.Readings[0].Value)
		})
	}
}

func TestInexactInt64ArrayElement(t *testing.T) {
	def := models.MetricDef{Name: "r", Datatype: uint32(protobuf.DataType_UInt64Array)}
	b, err := encodeArray(protobuf.DataType_UInt64Array, "[9007199254740992, 9007199254740993]")
	require.NoError(t, err)
	m := &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_BytesValue{BytesValue: b}}
	_, err = newReading(def, m, &protobuf.Payload{})
	assert.EqualError(t, err, "element 1 -> UInt64 value 9007199254740993 is beyond 2^53")

	def.Datatype = uint32(protobuf.DataType_Int64Array)
	b, err = encodeArray(protobuf.DataType_Int64Array, "[-9007199254740992, -9007199254740993]")
	require.NoError(t, err)
	m.Value = &protobuf.Payload_Metric_BytesValue{BytesValue: b}
	_, err = newReading(def, m, &protobuf.Payload{})
	assert.EqualError(t, err, "element 1 -> Int64 value -9007199254740993 is beyond ±2^53")
}
