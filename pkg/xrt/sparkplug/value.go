// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"cmp"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

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
	protobuf.DataType_Bytes:   common.ValueTypeBinary, // read only: EdgeX does not allow writing Binary

	protobuf.DataType_Int8Array:    common.ValueTypeInt8Array,
	protobuf.DataType_Int16Array:   common.ValueTypeInt16Array,
	protobuf.DataType_Int32Array:   common.ValueTypeInt32Array,
	protobuf.DataType_Int64Array:   common.ValueTypeInt64Array,
	protobuf.DataType_UInt8Array:   common.ValueTypeUint8Array,
	protobuf.DataType_UInt16Array:  common.ValueTypeUint16Array,
	protobuf.DataType_UInt32Array:  common.ValueTypeUint32Array,
	protobuf.DataType_UInt64Array:  common.ValueTypeUint64Array,
	protobuf.DataType_FloatArray:   common.ValueTypeFloat32Array,
	protobuf.DataType_DoubleArray:  common.ValueTypeFloat64Array,
	protobuf.DataType_BooleanArray: common.ValueTypeBoolArray,
	protobuf.DataType_StringArray:  common.ValueTypeStringArray,
}

// setValue accepts a string (e.g. from an EdgeX SET command), a Go number, or a bool; a number must fit the datatype.
func setValue(m *protobuf.Payload_Metric, dt protobuf.DataType, value any) errors.EdgeX {
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
			return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %#v is not a string", value), nil)
		}
		m.Value = &protobuf.Payload_Metric_StringValue{StringValue: s}
		return nil
	default:
		if _, ok := arrayElems[dt]; ok {
			b, err := encodeArray(dt, value)
			if err != nil {
				return err
			}
			m.Value = &protobuf.Payload_Metric_BytesValue{BytesValue: b}
			return nil
		}
		s, err := numberString(value)
		if err != nil {
			return err
		}
		return setNumber(m, dt, s)
	}
}

func setNumber(m *protobuf.Payload_Metric, dt protobuf.DataType, s string) errors.EdgeX {
	u, err := parseElem(dt, s)
	if err != nil {
		return err
	}
	switch dt {
	case protobuf.DataType_Int64, protobuf.DataType_UInt64:
		m.Value = &protobuf.Payload_Metric_LongValue{LongValue: u}
	case protobuf.DataType_Float:
		m.Value = &protobuf.Payload_Metric_FloatValue{FloatValue: math.Float32frombits(uint32(u))}
	case protobuf.DataType_Double:
		m.Value = &protobuf.Payload_Metric_DoubleValue{DoubleValue: math.Float64frombits(u)}
	default:
		m.Value = &protobuf.Payload_Metric_IntValue{IntValue: uint32(u)} // sign-extended, as Eclipse Tahu encodes it
	}
	return nil
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
func numberString(value any) (string, errors.EdgeX) {
	switch v := value.(type) {
	case string, json.Number, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return fmt.Sprint(v), nil
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32), nil
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), nil
	}
	return "", errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %#v is not a number", value), nil)
}

func toBool(value any) (bool, errors.EdgeX) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return false, errors.NewCommonEdgeX(errors.KindContractInvalid, "", err)
		}
		return b, nil
	}
	return false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %#v is not a bool", value), nil)
}

// maxExactInt is the largest integer magnitude a float64 holds exactly.
const maxExactInt = 1 << 53

// newReading converts a DACK metric that ackedMetrics accepted. Numbers become float64, as if decoded from the JSON of
// the XRT management API, so the xrtmodels conversions accept them; it fails for an integer float64 would change.
func newReading(def models.MetricDef, m *protobuf.Payload_Metric, dack *protobuf.Payload) (xrtmodels.Reading, errors.EdgeX) {
	dt := protobuf.DataType(def.Datatype)
	if err := checkExactAsFloat(dt, m); err != nil {
		return xrtmodels.Reading{}, err
	}
	value, _ := readingValue(dt, m)
	if isObject(def) {
		value = nil
		_ = json.Unmarshal([]byte(m.GetStringValue()), &value) // checked by validateResourceAck
	}
	// EdgeX requires an origin, in nanoseconds; a metric timestamp is optional.
	origin := int64(cmp.Or(m.GetTimestamp(), dack.GetTimestamp(), uint64(time.Now().UnixMilli()))) * int64(time.Millisecond)
	return xrtmodels.Reading{Value: value, Type: edgexValueType(def), Origin: origin}, nil
}

func edgexValueType(def models.MetricDef) string {
	if isObject(def) {
		return common.ValueTypeObject
	}
	return valueTypes[protobuf.DataType(def.Datatype)]
}

// objectContentType marks the Text metric of an EdgeX Object resource, whose value is JSON.
const objectContentType = "xrt/json-object"

func isObject(def models.MetricDef) bool {
	return protobuf.DataType(def.Datatype) == protobuf.DataType_Text && def.ContentType == objectContentType
}

// objectJSON returns the JSON of an Object value: a string must be JSON already, anything else is marshaled.
func objectJSON(value any) (string, errors.EdgeX) {
	if s, ok := value.(string); ok {
		if !json.Valid([]byte(s)) {
			return "", errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %q is not JSON", s), nil)
		}
		return s, nil
	}
	b, err := json.Marshal(value)
	if err != nil {
		return "", errors.NewCommonEdgeX(errors.KindContractInvalid, "", err)
	}
	return string(b), nil
}

func checkExactAsFloat(dt protobuf.DataType, m *protobuf.Payload_Metric) errors.EdgeX {
	switch dt {
	case protobuf.DataType_Int64:
		return checkExactInt64(int64(m.GetLongValue()))
	case protobuf.DataType_UInt64:
		return checkExactUint64(m.GetLongValue())
	case protobuf.DataType_Int64Array, protobuf.DataType_UInt64Array:
		b := m.GetBytesValue()
		for i := 0; i+8 <= len(b); i += 8 {
			u := binary.LittleEndian.Uint64(b[i:])
			var err errors.EdgeX
			if dt == protobuf.DataType_Int64Array {
				err = checkExactInt64(int64(u))
			} else {
				err = checkExactUint64(u)
			}
			if err != nil {
				return errors.NewCommonEdgeX(errors.Kind(err), fmt.Sprintf("element %d", i/8), err)
			}
		}
	}
	return nil
}

func checkExactInt64(v int64) errors.EdgeX {
	if v < -maxExactInt || v > maxExactInt {
		return errors.NewCommonEdgeX(errors.KindOverflowError, fmt.Sprintf("Int64 value %d is beyond ±2^53", v), nil)
	}
	return nil
}

func checkExactUint64(v uint64) errors.EdgeX {
	if v > maxExactInt {
		return errors.NewCommonEdgeX(errors.KindOverflowError, fmt.Sprintf("UInt64 value %d is beyond 2^53", v), nil)
	}
	return nil
}

// readingValue takes the value by the DBIRTH datatype, since a DACK metric has none.
func readingValue(dt protobuf.DataType, m *protobuf.Payload_Metric) (any, bool) {
	switch dt {
	case protobuf.DataType_Int8, protobuf.DataType_Int16, protobuf.DataType_Int32,
		protobuf.DataType_UInt8, protobuf.DataType_UInt16, protobuf.DataType_UInt32:
		return numberElem(dt, uint64(m.GetIntValue())), hasValue[*protobuf.Payload_Metric_IntValue](m)
	case protobuf.DataType_Int64, protobuf.DataType_UInt64:
		return numberElem(dt, m.GetLongValue()), hasValue[*protobuf.Payload_Metric_LongValue](m)
	case protobuf.DataType_Float:
		return float64(m.GetFloatValue()), hasValue[*protobuf.Payload_Metric_FloatValue](m)
	case protobuf.DataType_Double:
		return m.GetDoubleValue(), hasValue[*protobuf.Payload_Metric_DoubleValue](m)
	case protobuf.DataType_Boolean:
		return m.GetBooleanValue(), hasValue[*protobuf.Payload_Metric_BooleanValue](m)
	case protobuf.DataType_String, protobuf.DataType_Text:
		return m.GetStringValue(), hasValue[*protobuf.Payload_Metric_StringValue](m)
	case protobuf.DataType_Bytes:
		return base64.StdEncoding.EncodeToString(m.GetBytesValue()), hasValue[*protobuf.Payload_Metric_BytesValue](m)
	}
	if _, ok := arrayElems[dt]; ok && hasValue[*protobuf.Payload_Metric_BytesValue](m) {
		return decodeArray(dt, m.GetBytesValue())
	}
	return nil, false
}

func hasValue[T any](m *protobuf.Payload_Metric) bool {
	_, ok := m.GetValue().(T)
	return ok
}
