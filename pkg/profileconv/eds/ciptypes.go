// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// cipValueType maps a CIP data-type code to its EdgeX profile valueType, using
// the core-contracts constants so the spelling matches what the profile
// validator accepts. Codes follow the ODVA CIP elementary-data-type table.
//
// Bit-string types (BYTE/WORD/DWORD/LWORD) map to the unsigned integer of the
// same width, matching how XRT profiles treat them.
//
// Deliberately NOT in this map — valueTypeForCIP errors on all of these rather
// than guess a valueType:
//   - EPATH (0xDC): a CIP path, not a data point.
//   - Time/date types (0xC0, 0xCC–0xCF, 0xD6–0xD8, 0xDB, 0xDF): rare in I/O.
//   - STRING2 (0xD5), STRINGN (0xD9), STRINGI (0xDE): distinct string variants
//     with 2-byte/variable-width/structured encodings. They are NOT decodable as
//     a plain STRING, so mapping them to String would yield a profile that reads
//     garbled data. They are rare in fixed-I/O EDS; erroring surfaces the param
//     rather than silently producing a wrong profile.
//   - ENGUNIT (0xDD): engineering-unit code; rare.
//
// Array valueTypes (Uint8Array, Uint16Array…) and Binary/Object are also absent:
// array-ness is not carried by a scalar CIP code — it comes from the assembly
// layout / bitLength or a logixTag's arraySize, which set the valueType there,
// not in this table.
var cipValueType = map[uint8]string{
	0xC1: common.ValueTypeBool,    // BOOL
	0xC2: common.ValueTypeInt8,    // SINT
	0xC3: common.ValueTypeInt16,   // INT
	0xC4: common.ValueTypeInt32,   // DINT
	0xC5: common.ValueTypeInt64,   // LINT
	0xC6: common.ValueTypeUint8,   // USINT
	0xC7: common.ValueTypeUint16,  // UINT
	0xC8: common.ValueTypeUint32,  // UDINT
	0xC9: common.ValueTypeUint64,  // ULINT
	0xCA: common.ValueTypeFloat32, // REAL
	0xCB: common.ValueTypeFloat64, // LREAL
	0xD0: common.ValueTypeString,  // STRING
	0xDA: common.ValueTypeString,  // SHORT_STRING
	0xD1: common.ValueTypeUint8,   // BYTE  (8-bit collection)
	0xD2: common.ValueTypeUint16,  // WORD  (16-bit collection)
	0xD3: common.ValueTypeUint32,  // DWORD (32-bit collection)
	0xD4: common.ValueTypeUint64,  // LWORD (64-bit collection)
}

// valueTypeForCIP returns the EdgeX valueType for a CIP data-type code as it
// appears in an EDS [Params] entry (decimal or "0x" hex, e.g. "0xC3" or "195").
// It errors on an unparseable code or one with no scalar valueType (EPATH,
// time/date types), so callers surface the offending param rather than guessing.
func valueTypeForCIP(code string) (string, errors.EdgeX) {
	n, err := parseCIPCode(code)
	if err != nil {
		return "", err
	}
	vt, ok := cipValueType[n]
	if !ok {
		return "", errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("unsupported CIP data type code 0x%02X", n), nil)
	}
	return vt, nil
}

// parseCIPCode parses a CIP data-type code that may be written as decimal or
// "0x"-prefixed hex, and must fit in a byte.
func parseCIPCode(code string) (uint8, errors.EdgeX) {
	s := strings.TrimSpace(code)
	if s == "" {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, "empty CIP data type code", nil)
	}
	// base 0 lets strconv honour a "0x" prefix as hex and bare digits as decimal.
	n, err := strconv.ParseUint(s, 0, 8)
	if err != nil {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("invalid CIP data type code %q", code), err)
	}
	return uint8(n), nil
}
