// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

// arrayElems maps each array datatype a DCMD supports to its element datatype. An array is the bytes_value of packed
// little-endian elements (spec 3.0.0, 6.4.17); the spec's FloatArray and DoubleArray examples are big-endian, but
// Eclipse Tahu, the reference implementation, encodes them little-endian like the text says.
var arrayElems = map[protobuf.DataType]protobuf.DataType{
	protobuf.DataType_Int8Array:    protobuf.DataType_Int8,
	protobuf.DataType_Int16Array:   protobuf.DataType_Int16,
	protobuf.DataType_Int32Array:   protobuf.DataType_Int32,
	protobuf.DataType_Int64Array:   protobuf.DataType_Int64,
	protobuf.DataType_UInt8Array:   protobuf.DataType_UInt8,
	protobuf.DataType_UInt16Array:  protobuf.DataType_UInt16,
	protobuf.DataType_UInt32Array:  protobuf.DataType_UInt32,
	protobuf.DataType_UInt64Array:  protobuf.DataType_UInt64,
	protobuf.DataType_FloatArray:   protobuf.DataType_Float,
	protobuf.DataType_DoubleArray:  protobuf.DataType_Double,
	protobuf.DataType_BooleanArray: protobuf.DataType_Boolean,
	protobuf.DataType_StringArray:  protobuf.DataType_String,
}

// encodeArray encodes value, a JSON array string (e.g. from an EdgeX SET command) or a slice, as an array datatype.
func encodeArray(dt protobuf.DataType, value any) ([]byte, errors.EdgeX) {
	elems, err := arrayElements(value)
	if err != nil {
		return nil, err
	}
	elem := arrayElems[dt]
	if elem == protobuf.DataType_Boolean {
		return encodeBools(elems)
	}
	var b []byte
	for i, e := range elems {
		if b, err = appendElem(b, elem, e); err != nil {
			return nil, errors.NewCommonEdgeX(errors.Kind(err), fmt.Sprintf("element %d", i), err)
		}
	}
	return b, nil
}

func arrayElements(value any) ([]any, errors.EdgeX) {
	if s, ok := value.(string); ok {
		d := json.NewDecoder(strings.NewReader(s))
		d.UseNumber() // keeps each number exact for the range check of its element type
		var elems []any
		if err := d.Decode(&elems); err != nil || elems == nil { // null decodes to a nil slice
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %q is not a JSON array", s), nil)
		}
		if _, err := d.Token(); err != io.EOF {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %q has data after its JSON array", s), nil)
		}
		return elems, nil
	}
	v := reflect.ValueOf(value)
	if v.Kind() != reflect.Slice && v.Kind() != reflect.Array {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %#v is not an array", value), nil)
	}
	elems := make([]any, v.Len())
	for i := range elems {
		elems[i] = v.Index(i).Interface()
	}
	return elems, nil
}

func appendElem(b []byte, elem protobuf.DataType, v any) ([]byte, errors.EdgeX) {
	if elem == protobuf.DataType_String {
		s, ok := v.(string)
		if !ok || strings.IndexByte(s, 0) >= 0 {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("value %#v is not a string without NUL", v), nil)
		}
		return append(append(b, s...), 0), nil
	}
	s, err := numberString(v)
	if err != nil {
		return nil, err
	}
	u, err := parseElem(elem, s)
	if err != nil {
		return nil, err
	}
	// A narrower element is the first bytes of its little-endian uint64.
	return binary.LittleEndian.AppendUint64(b, u)[:len(b)+bitSize(elem)/8], nil
}

// parseElem returns the bits of a number, range checked for its datatype; a signed one is sign-extended.
func parseElem(dt protobuf.DataType, s string) (uint64, errors.EdgeX) {
	bits := bitSize(dt)
	var u uint64
	var err error
	switch dt {
	case protobuf.DataType_Int8, protobuf.DataType_Int16, protobuf.DataType_Int32, protobuf.DataType_Int64:
		var i int64
		i, err = strconv.ParseInt(s, 10, bits)
		u = uint64(i)
	case protobuf.DataType_UInt8, protobuf.DataType_UInt16, protobuf.DataType_UInt32, protobuf.DataType_UInt64:
		u, err = strconv.ParseUint(s, 10, bits)
	case protobuf.DataType_Float:
		var f float64
		f, err = strconv.ParseFloat(s, bits)
		u = uint64(math.Float32bits(float32(f)))
	case protobuf.DataType_Double:
		var f float64
		f, err = strconv.ParseFloat(s, bits)
		u = math.Float64bits(f)
	default:
		return 0, errors.NewCommonEdgeX(errors.KindNotImplemented, fmt.Sprintf("datatype %s is not supported", dt), nil)
	}
	if err != nil {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, "", err) // no message of its own, so it reads as err
	}
	return u, nil
}

// encodeBools writes the element count, then the elements bit-packed with the first in the most significant bit, as
// the spec example and Eclipse Tahu do.
func encodeBools(elems []any) ([]byte, errors.EdgeX) {
	b := binary.LittleEndian.AppendUint32(nil, uint32(len(elems)))
	b = append(b, make([]byte, (len(elems)+7)/8)...)
	for i, e := range elems {
		v, err := toBool(e)
		if err != nil {
			return nil, errors.NewCommonEdgeX(errors.Kind(err), fmt.Sprintf("element %d", i), err)
		}
		if v {
			b[4+i/8] |= 0x80 >> (i % 8)
		}
	}
	return b, nil
}

// decodeArray decodes the bytes_value of an array datatype; it reports false if b does not hold whole elements.
// Numbers become float64, as xrtmodels expects of array elements too.
func decodeArray(dt protobuf.DataType, b []byte) ([]any, bool) {
	elem := arrayElems[dt]
	switch elem {
	case protobuf.DataType_Boolean:
		return decodeBools(b)
	case protobuf.DataType_String:
		return decodeStrings(b)
	}
	width := bitSize(elem) / 8
	if len(b)%width != 0 {
		return nil, false
	}
	elems := make([]any, 0, len(b)/width)
	for i := 0; i < len(b); i += width {
		elems = append(elems, numberElem(elem, littleEndian(b[i:i+width])))
	}
	return elems, true
}

func littleEndian(b []byte) uint64 {
	var buf [8]byte
	copy(buf[:], b)
	return binary.LittleEndian.Uint64(buf[:])
}

// numberElem converts the bits of a number to float64, truncated to the width of its datatype.
func numberElem(dt protobuf.DataType, u uint64) float64 {
	switch dt {
	case protobuf.DataType_Int8:
		return float64(int8(u))
	case protobuf.DataType_Int16:
		return float64(int16(u))
	case protobuf.DataType_Int32:
		return float64(int32(u))
	case protobuf.DataType_Int64:
		return float64(int64(u))
	case protobuf.DataType_UInt8:
		return float64(uint8(u))
	case protobuf.DataType_UInt16:
		return float64(uint16(u))
	case protobuf.DataType_UInt32:
		return float64(uint32(u))
	case protobuf.DataType_Float:
		return float64(math.Float32frombits(uint32(u)))
	case protobuf.DataType_Double:
		return math.Float64frombits(u)
	}
	return float64(u)
}

func decodeBools(b []byte) ([]any, bool) {
	if len(b) < 4 {
		return nil, false
	}
	// uint64, since the count can overflow an int on a 32-bit platform
	n := binary.LittleEndian.Uint32(b)
	if uint64(len(b)) != 4+(uint64(n)+7)/8 {
		return nil, false
	}
	elems := make([]any, n)
	for i := range n {
		elems[i] = b[4+i/8]&(0x80>>(i%8)) != 0
	}
	return elems, true
}

func decodeStrings(b []byte) ([]any, bool) {
	if len(b) == 0 {
		return []any{}, true
	}
	if b[len(b)-1] != 0 {
		return nil, false
	}
	parts := bytes.Split(b[:len(b)-1], []byte{0})
	elems := make([]any, len(parts))
	for i, p := range parts {
		elems[i] = string(p)
	}
	return elems, true
}
