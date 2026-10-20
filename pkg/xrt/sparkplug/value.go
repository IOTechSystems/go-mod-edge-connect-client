// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"cmp"
	"fmt"
	"strconv"
	"time"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

// valueTypes are the Sparkplug datatypes a DCMD can read or write, with the EdgeX value type of their readings.
var valueTypes = map[protobuf.DataType]string{
	protobuf.DataType_Int8:    common.ValueTypeInt8,
	protobuf.DataType_Int16:   common.ValueTypeInt16,
	protobuf.DataType_Int32:   common.ValueTypeInt32,
	protobuf.DataType_Int64:   common.ValueTypeInt64,
	protobuf.DataType_UInt8:   common.ValueTypeUint8,
	protobuf.DataType_UInt16:  common.ValueTypeUint16,
	protobuf.DataType_UInt32:  common.ValueTypeUint32,
	protobuf.DataType_UInt64:  common.ValueTypeUint64,
	protobuf.DataType_Float:   common.ValueTypeFloat32,
	protobuf.DataType_Double:  common.ValueTypeFloat64,
	protobuf.DataType_Boolean: common.ValueTypeBool,
	protobuf.DataType_String:  common.ValueTypeString,
	protobuf.DataType_Text:    common.ValueTypeString,
}

// setValue accepts a string (e.g. from an EdgeX SET command), a Go number, or a bool; a number must fit the datatype.
func setValue(m *protobuf.Payload_Metric, dt protobuf.DataType, value any) error {
	switch dt {
	case protobuf.DataType_Boolean:
		b, err := toBool(value)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_BooleanValue{BooleanValue: b}
		return nil
	case protobuf.DataType_String, protobuf.DataType_Text:
		s, ok := value.(string)
		if !ok {
			return fmt.Errorf("%T is not a string", value)
		}
		m.Value = &protobuf.Payload_Metric_StringValue{StringValue: s}
		return nil
	default:
		s, err := numberString(value)
		if err != nil {
			return err
		}
		return setNumber(m, dt, s)
	}
}

func setNumber(m *protobuf.Payload_Metric, dt protobuf.DataType, s string) error {
	bits := bitSize(dt)
	switch dt {
	case protobuf.DataType_Int8, protobuf.DataType_Int16, protobuf.DataType_Int32:
		i, err := strconv.ParseInt(s, 10, bits)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_IntValue{IntValue: uint32(i)} // sign-extended, as Eclipse Tahu encodes it
		return nil
	case protobuf.DataType_Int64:
		i, err := strconv.ParseInt(s, 10, bits)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_LongValue{LongValue: uint64(i)}
		return nil
	case protobuf.DataType_UInt8, protobuf.DataType_UInt16, protobuf.DataType_UInt32:
		u, err := strconv.ParseUint(s, 10, bits)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_IntValue{IntValue: uint32(u)}
		return nil
	case protobuf.DataType_UInt64:
		u, err := strconv.ParseUint(s, 10, bits)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_LongValue{LongValue: u}
		return nil
	case protobuf.DataType_Float:
		f, err := strconv.ParseFloat(s, bits)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_FloatValue{FloatValue: float32(f)}
		return nil
	case protobuf.DataType_Double:
		f, err := strconv.ParseFloat(s, bits)
		if err != nil {
			return err
		}
		m.Value = &protobuf.Payload_Metric_DoubleValue{DoubleValue: f}
		return nil
	}
	return fmt.Errorf("datatype %s is not supported", dt)
}

func bitSize(dt protobuf.DataType) int {
	switch dt {
	case protobuf.DataType_Int8, protobuf.DataType_UInt8:
		return 8
	case protobuf.DataType_Int16, protobuf.DataType_UInt16:
		return 16
	case protobuf.DataType_Int32, protobuf.DataType_UInt32, protobuf.DataType_Float:
		return 32
	}
	return 64
}

// numberString formats value in decimal, so strconv parses it with its range check whatever its Go type is.
func numberString(value any) (string, error) {
	switch v := value.(type) {
	case string, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	}
	return "", fmt.Errorf("%T is not a number", value)
}

func toBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return strconv.ParseBool(v)
	}
	return false, fmt.Errorf("%T is not a bool", value)
}

// maxExactInt is the largest integer magnitude a float64 holds exactly.
const maxExactInt = 1 << 53

// newReading converts a DACK metric that ackedMetrics accepted. Numbers become float64, as if decoded from the JSON of
// the XRT management API, so the xrtmodels conversions accept them; it fails for an integer float64 would change.
func newReading(def models.MetricDef, m *protobuf.Payload_Metric, ack *protobuf.Payload) (xrtmodels.Reading, error) {
	dt := protobuf.DataType(def.Datatype)
	if err := checkExactAsFloat(dt, m); err != nil {
		return xrtmodels.Reading{}, err
	}
	value, _ := readingValue(dt, m)
	// EdgeX requires an origin, in nanoseconds; a metric timestamp is optional.
	origin := int64(cmp.Or(m.GetTimestamp(), ack.GetTimestamp())) * int64(time.Millisecond)
	return xrtmodels.Reading{Value: value, Type: valueTypes[dt], Origin: origin}, nil
}

func checkExactAsFloat(dt protobuf.DataType, m *protobuf.Payload_Metric) error {
	switch dt {
	case protobuf.DataType_Int64:
		if v := int64(m.GetLongValue()); v < -maxExactInt || v > maxExactInt {
			return fmt.Errorf("Int64 value %d is beyond ±2^53", v)
		}
	case protobuf.DataType_UInt64:
		if v := m.GetLongValue(); v > maxExactInt {
			return fmt.Errorf("UInt64 value %d is beyond 2^53", v)
		}
	}
	return nil
}

// readingValue takes the value by the DBIRTH datatype, since a DACK metric has none.
func readingValue(dt protobuf.DataType, m *protobuf.Payload_Metric) (any, bool) {
	switch dt {
	case protobuf.DataType_Int8:
		return float64(int8(m.GetIntValue())), hasValue[*protobuf.Payload_Metric_IntValue](m)
	case protobuf.DataType_Int16:
		return float64(int16(m.GetIntValue())), hasValue[*protobuf.Payload_Metric_IntValue](m)
	case protobuf.DataType_Int32:
		return float64(int32(m.GetIntValue())), hasValue[*protobuf.Payload_Metric_IntValue](m)
	case protobuf.DataType_UInt8, protobuf.DataType_UInt16, protobuf.DataType_UInt32:
		return float64(m.GetIntValue()), hasValue[*protobuf.Payload_Metric_IntValue](m)
	case protobuf.DataType_Int64:
		return float64(int64(m.GetLongValue())), hasValue[*protobuf.Payload_Metric_LongValue](m)
	case protobuf.DataType_UInt64:
		return float64(m.GetLongValue()), hasValue[*protobuf.Payload_Metric_LongValue](m)
	case protobuf.DataType_Float:
		return float64(m.GetFloatValue()), hasValue[*protobuf.Payload_Metric_FloatValue](m)
	case protobuf.DataType_Double:
		return m.GetDoubleValue(), hasValue[*protobuf.Payload_Metric_DoubleValue](m)
	case protobuf.DataType_Boolean:
		return m.GetBooleanValue(), hasValue[*protobuf.Payload_Metric_BooleanValue](m)
	case protobuf.DataType_String, protobuf.DataType_Text:
		return m.GetStringValue(), hasValue[*protobuf.Payload_Metric_StringValue](m)
	}
	return nil, false
}

func hasValue[T any](m *protobuf.Payload_Metric) bool {
	_, ok := m.GetValue().(T)
	return ok
}
