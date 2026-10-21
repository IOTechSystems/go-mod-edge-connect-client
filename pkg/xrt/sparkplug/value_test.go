// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"math"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

func TestSetValue(t *testing.T) {
	tests := []struct {
		dt    protobuf.DataType
		value any
		want  any // the metric value, or nil if value is rejected
	}{
		{protobuf.DataType_Int8, "-128", uint32(0xFFFFFF80)},
		{protobuf.DataType_Int8, int8(127), uint32(127)},
		{protobuf.DataType_Int8, 128, nil},
		{protobuf.DataType_Int8, float64(-129), nil},
		{protobuf.DataType_Int16, "-32768", uint32(0xFFFF8000)},
		{protobuf.DataType_Int16, "32768", nil},
		{protobuf.DataType_Int32, float64(math.MaxInt32), uint32(math.MaxInt32)},
		{protobuf.DataType_Int32, float64(math.MaxInt32 + 1), nil},
		{protobuf.DataType_Int32, 3.5, nil},
		{protobuf.DataType_Int64, "-9223372036854775808", uint64(1 << 63)},
		{protobuf.DataType_Int64, "9223372036854775808", nil},
		{protobuf.DataType_UInt8, uint8(255), uint32(255)},
		{protobuf.DataType_UInt8, 256, nil},
		{protobuf.DataType_UInt8, -1, nil},
		{protobuf.DataType_UInt16, "65535", uint32(65535)},
		{protobuf.DataType_UInt16, "65536", nil},
		{protobuf.DataType_UInt32, uint64(math.MaxUint32), uint32(math.MaxUint32)},
		{protobuf.DataType_UInt32, uint64(math.MaxUint32 + 1), nil},
		{protobuf.DataType_UInt64, "18446744073709551615", uint64(math.MaxUint64)},
		{protobuf.DataType_UInt64, "18446744073709551616", nil},
		{protobuf.DataType_Float, "3.4e38", float32(3.4e38)},
		{protobuf.DataType_Float, "3.5e38", nil},
		{protobuf.DataType_Float, 1, float32(1)},
		{protobuf.DataType_Double, float32(0.5), 0.5},
		{protobuf.DataType_Double, "1e309", nil},
		{protobuf.DataType_Double, true, nil},
		{protobuf.DataType_Boolean, "true", true},
		{protobuf.DataType_Boolean, false, false},
		{protobuf.DataType_Boolean, 1, nil},
		{protobuf.DataType_String, "abc", "abc"},
		{protobuf.DataType_Text, 1, nil},
		{protobuf.DataType_UUID, "abc", nil},
	}
	for _, tt := range tests {
		m := &protobuf.Payload_Metric{}
		err := setValue(m, tt.dt, tt.value)
		if tt.want == nil {
			assert.Error(t, err, "%s %#v", tt.dt, tt.value)
			assert.Nil(t, m.GetValue(), "a rejected value must not be set: %s %#v", tt.dt, tt.value)
			continue
		}
		require.NoError(t, err, "%s %#v", tt.dt, tt.value)
		assert.Equal(t, tt.want, metricValue(m), "%s %#v", tt.dt, tt.value)
	}
}

func metricValue(m *protobuf.Payload_Metric) any {
	switch v := m.GetValue().(type) {
	case *protobuf.Payload_Metric_IntValue:
		return v.IntValue
	case *protobuf.Payload_Metric_LongValue:
		return v.LongValue
	case *protobuf.Payload_Metric_FloatValue:
		return v.FloatValue
	case *protobuf.Payload_Metric_DoubleValue:
		return v.DoubleValue
	case *protobuf.Payload_Metric_BooleanValue:
		return v.BooleanValue
	case *protobuf.Payload_Metric_StringValue:
		return v.StringValue
	}
	return nil
}

func TestReadingValue(t *testing.T) {
	intValue := func(v uint32) *protobuf.Payload_Metric {
		return &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_IntValue{IntValue: v}}
	}
	longValue := func(v uint64) *protobuf.Payload_Metric {
		return &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_LongValue{LongValue: v}}
	}
	tests := []struct {
		name string
		dt   protobuf.DataType
		m    *protobuf.Payload_Metric
		want any
	}{
		{"Int8 sign-extended", protobuf.DataType_Int8, intValue(0xFFFFFF80), float64(-128)},
		{"Int8 not sign-extended", protobuf.DataType_Int8, intValue(0x80), float64(-128)},
		{"Int16", protobuf.DataType_Int16, intValue(0xFFFF8000), float64(-32768)},
		{"Int32", protobuf.DataType_Int32, intValue(0x80000000), float64(math.MinInt32)},
		{"Int64", protobuf.DataType_Int64, longValue(math.MaxUint64), float64(-1)},
		{"UInt8", protobuf.DataType_UInt8, intValue(255), float64(255)},
		{"UInt8 beyond its width", protobuf.DataType_UInt8, intValue(0x1FF), float64(255)},
		{"UInt16 beyond its width", protobuf.DataType_UInt16, intValue(0x1FFFF), float64(65535)},
		{"UInt32", protobuf.DataType_UInt32, intValue(math.MaxUint32), float64(math.MaxUint32)},
		{"UInt64", protobuf.DataType_UInt64, longValue(1 << 63), float64(1 << 63)},
		{"Float", protobuf.DataType_Float, &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_FloatValue{FloatValue: 0.5}}, 0.5},
		{"Double", protobuf.DataType_Double, &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_DoubleValue{DoubleValue: 0.25}}, 0.25},
		{"Boolean", protobuf.DataType_Boolean, &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_BooleanValue{BooleanValue: true}}, true},
		{"Text", protobuf.DataType_Text, &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_StringValue{StringValue: "abc"}}, "abc"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, ok := readingValue(tt.dt, tt.m)
			require.True(t, ok)
			assert.Equal(t, tt.want, value)
		})
	}
}

func TestReadingValueMismatch(t *testing.T) {
	_, ok := readingValue(protobuf.DataType_Float, &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_IntValue{IntValue: 1}})
	assert.False(t, ok, "a value in another oneof field must not read as zero")
	_, ok = readingValue(protobuf.DataType_Int64, &protobuf.Payload_Metric{IsNull: proto.Bool(true)})
	assert.False(t, ok)
	_, ok = readingValue(protobuf.DataType_UInt8Array, &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_IntValue{IntValue: 1}})
	assert.False(t, ok, "an array without bytes_value must not read as empty")
}

func TestNewReadingOrigin(t *testing.T) {
	def := models.MetricDef{Name: "r", Datatype: uint32(protobuf.DataType_Float)}
	ack := &protobuf.Payload{Timestamp: proto.Uint64(2)}
	m := &protobuf.Payload_Metric{Timestamp: proto.Uint64(1), Value: &protobuf.Payload_Metric_FloatValue{FloatValue: 1}}
	reading, err := newReading(def, m, ack)
	require.NoError(t, err)
	assert.Equal(t, int64(1_000_000), reading.Origin)
	m.Timestamp = nil
	reading, err = newReading(def, m, ack)
	require.NoError(t, err)
	assert.Equal(t, int64(2_000_000), reading.Origin, "a metric without a timestamp takes the DACK one")

	before := time.Now().UnixNano()
	reading, err = newReading(def, m, &protobuf.Payload{})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, reading.Origin, before-int64(time.Millisecond), "without any timestamp, the origin is now")
}

func TestNewReadingInexactInteger(t *testing.T) {
	tests := []struct {
		dt    protobuf.DataType
		value uint64
		err   string // empty if the value converts exactly
	}{
		{protobuf.DataType_Int64, 1 << 53, ""},
		{protobuf.DataType_Int64, 1<<53 + 1, "Int64 value 9007199254740993 is beyond ±2^53"},
		{protobuf.DataType_Int64, uint64(-(1 << 53) & math.MaxUint64), ""},
		{protobuf.DataType_Int64, uint64((-(1 << 53) - 1) & math.MaxUint64), "Int64 value -9007199254740993 is beyond ±2^53"},
		{protobuf.DataType_Int64, math.MaxInt64, "Int64 value 9223372036854775807 is beyond ±2^53"},
		{protobuf.DataType_UInt64, 1 << 53, ""},
		{protobuf.DataType_UInt64, 1<<53 + 1, "UInt64 value 9007199254740993 is beyond 2^53"},
		{protobuf.DataType_UInt64, math.MaxUint64, "UInt64 value 18446744073709551615 is beyond 2^53"},
	}
	for _, tt := range tests {
		def := models.MetricDef{Name: "r", Datatype: uint32(tt.dt)}
		m := &protobuf.Payload_Metric{Value: &protobuf.Payload_Metric_LongValue{LongValue: tt.value}}
		_, err := newReading(def, m, &protobuf.Payload{})
		if tt.err == "" {
			assert.NoError(t, err, "%s %d", tt.dt, tt.value)
			continue
		}
		assert.EqualError(t, err, tt.err, "%s %d", tt.dt, tt.value)
	}
}

var objectDef = models.MetricDef{Name: "obj", Alias: 1, Datatype: uint32(protobuf.DataType_Text), ContentType: "xrt/json-object"}

func TestObjectJSON(t *testing.T) {
	s, err := objectJSON(`{"a": 2}`)
	require.NoError(t, err)
	assert.Equal(t, `{"a": 2}`, s)
	s, err = objectJSON(map[string]any{"a": 2, "b": "c"})
	require.NoError(t, err)
	assert.Equal(t, `{"a":2,"b":"c"}`, s)
	_, err = objectJSON("abc")
	assert.Error(t, err)
	// Unlike other datatypes, an Object takes null.
	s, err = objectJSON(nil)
	require.NoError(t, err)
	assert.Equal(t, "null", s)
}

func TestWriteMetricObject(t *testing.T) {
	m, err := writeMetric(objectDef, map[string]any{"a": 2})
	require.NoError(t, err)
	assert.Equal(t, `{"a":2}`, m.GetStringValue())

	_, err = writeMetric(objectDef, "abc")
	assert.EqualError(t, err, `invalid Object value abc for obj -> value "abc" is not JSON`)
	textDef := objectDef
	textDef.ContentType = ""
	_, err = writeMetric(textDef, map[string]any{"a": 2})
	assert.Error(t, err, "only an Object takes a map")
}

func TestObjectReadingConvertsToEdgeXEvent(t *testing.T) {
	m := &protobuf.Payload_Metric{Timestamp: proto.Uint64(1), Value: &protobuf.Payload_Metric_StringValue{StringValue: `{"a":1}`}}
	reading, err := newReading(objectDef, m, &protobuf.Payload{})
	require.NoError(t, err)
	assert.Equal(t, xrtmodels.Reading{Value: map[string]any{"a": 1.0}, Type: common.ValueTypeObject, Origin: 1_000_000}, reading)
	event, err := xrtmodels.ToEdgeXV2EventDTO(xrtmodels.MultiResourcesResult{Device: "d", Readings: map[string]xrtmodels.Reading{"obj": reading}})
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": 1.0}, event.Readings[0].ObjectValue)

	textDef := objectDef
	textDef.ContentType = ""
	reading, err = newReading(textDef, m, &protobuf.Payload{})
	require.NoError(t, err)
	assert.Equal(t, `{"a":1}`, reading.Value, "a Text without the Object content type stays a string")
	assert.Equal(t, common.ValueTypeString, reading.Type)
}

func TestBinaryReadingConvertsToEdgeXEvent(t *testing.T) {
	def := models.MetricDef{Name: "bin", Datatype: uint32(protobuf.DataType_Bytes)}
	m := &protobuf.Payload_Metric{Timestamp: proto.Uint64(1), Value: &protobuf.Payload_Metric_BytesValue{BytesValue: []byte{1, 2, 3}}}
	reading, err := newReading(def, m, &protobuf.Payload{})
	require.NoError(t, err)
	assert.Equal(t, xrtmodels.Reading{Value: "AQID", Type: common.ValueTypeBinary, Origin: 1_000_000}, reading)
	event, err := xrtmodels.ToEdgeXV2EventDTO(xrtmodels.MultiResourcesResult{Device: "d", Readings: map[string]xrtmodels.Reading{"bin": reading}})
	require.NoError(t, err)
	assert.Equal(t, []byte{1, 2, 3}, event.Readings[0].BinaryValue)
}

func TestInvalidValueKeepsTheParseMessage(t *testing.T) {
	_, err := writeMetric(models.MetricDef{Name: "Voltage", Datatype: uint32(protobuf.DataType_UInt16)}, "abc")
	assert.Equal(t, errors.KindContractInvalid, errors.Kind(err))
	assert.EqualError(t, err, `invalid value abc for Voltage of type Uint16 -> strconv.ParseUint: parsing "abc": invalid syntax`)

	_, err = writeMetric(models.MetricDef{Name: "Codes", Datatype: uint32(protobuf.DataType_Int8Array)}, "[1, 128]")
	assert.Equal(t, errors.KindContractInvalid, errors.Kind(err))
	assert.EqualError(t, err, `invalid value [1, 128] for Codes of type Int8Array -> element 1 -> strconv.ParseInt: parsing "128": value out of range`)
}
