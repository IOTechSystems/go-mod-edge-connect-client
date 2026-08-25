// Copyright (C) 2026 IOTech Ltd

package tia

import "github.com/edgexfoundry/go-mod-core-contracts/v4/common"

// S7 data-type tables. Every entry decides both a resource's valueType and how
// far the cursor advances, so a wrong number silently misaligns everything after
// it — hence the authority note on each figure that was in doubt.

// typeAliases maps the long IEC 61131-3 spellings onto the short ones. They name
// the same type (IEC 61131-3 3rd Ed. Table 10, entries 14a/14b and 15a/15b) and
// TIA Portal accepts either, so an export may use either.
//
// Without this the same type takes a different path per spelling — one emitting a
// resource, the other skipped or unrecognised, leaving the cursor stranded.
var typeAliases = map[string]string{
	"TIME_OF_DAY":    "TOD",
	"LTIME_OF_DAY":   "LTOD",
	"DATE_AND_TIME":  "DT",
	"DATE_AND_LTIME": "LDT", // Siemens spelling
	"LDATE_AND_TIME": "LDT", // IEC spelling
}

// normaliseType resolves an alias to the spelling the tables are keyed by. Every
// lookup must go through it, or a type is handled differently per spelling.
func normaliseType(s7Type string) string {
	if canonical, ok := typeAliases[s7Type]; ok {
		return canonical
	}
	return s7Type
}

// ─── Type mapping tables ──────────────────────────────────────────────────────

type scalarInfo struct {
	valueType string
	byteSize  int
	alignment int // 0 = any byte boundary, 2 = word (even-byte) boundary
	// maximum bounds a type whose meaning is narrower than the integer carrying
	// it — TOD is a time of day in a Uint32, so only 0..86_399_999 is a real
	// time. XRT enforces it on write. Zero means no bound is emitted.
	maximum float64
}

// The BOOL entry is never looked up: emit intercepts a Bool before any table,
// because bit-packing needs the cursor rather than a size. It is listed so the
// table records the whole scalar type system.
// Columns: valueType, byteSize, alignment, maximum.
var scalarTypes = map[string]scalarInfo{
	"BOOL":  {common.ValueTypeBool, 0, 0, 0},
	"SINT":  {common.ValueTypeInt8, 1, 0, 0},
	"USINT": {common.ValueTypeUint8, 1, 0, 0},
	"BYTE":  {common.ValueTypeUint8, 1, 0, 0},
	"INT":   {common.ValueTypeInt16, 2, 2, 0},
	"UINT":  {common.ValueTypeUint16, 2, 2, 0},
	"WORD":  {common.ValueTypeUint16, 2, 2, 0},
	"DINT":  {common.ValueTypeInt32, 4, 2, 0},
	"UDINT": {common.ValueTypeUint32, 4, 2, 0},
	"DWORD": {common.ValueTypeUint32, 4, 2, 0},
	"TIME":  {common.ValueTypeInt32, 4, 2, 0},
	// A day count from 1990-01-01, passed through as a plain uint16 (Checklist:
	// "We don't convert the uint16 to a date"). Its documented range
	// D#1990-01-01..D#2169-06-06 is exactly 0..65535, so it needs no maximum.
	"DATE": {common.ValueTypeUint16, 2, 2, 0},
	// TOD holds milliseconds since midnight, so it is unsigned and capped at
	// 23:59:59.999; TIME, two lines up, is a signed interval and can be negative.
	"TOD":   {common.ValueTypeUint32, 4, 2, 86_399_999},
	"LINT":  {common.ValueTypeInt64, 8, 2, 0},
	"ULINT": {common.ValueTypeUint64, 8, 2, 0},
	"LWORD": {common.ValueTypeUint64, 8, 2, 0},
	// LTOD is the nanosecond counterpart, capped at 23:59:59.999999999.
	"LTOD":  {common.ValueTypeUint64, 8, 2, 86_399_999_999_999},
	"REAL":  {common.ValueTypeFloat32, 4, 2, 0},
	"LREAL": {common.ValueTypeFloat64, 8, 2, 0},
}

// arrayTypes maps a scalar valueType to its array form. The Bool entry is
// likewise unreachable — emitArray packs a Bool array itself — and is kept so
// every scalar has a counterpart.
var arrayTypes = map[string]string{
	common.ValueTypeInt8:    common.ValueTypeInt8Array,
	common.ValueTypeUint8:   common.ValueTypeUint8Array,
	common.ValueTypeInt16:   common.ValueTypeInt16Array,
	common.ValueTypeUint16:  common.ValueTypeUint16Array,
	common.ValueTypeInt32:   common.ValueTypeInt32Array,
	common.ValueTypeUint32:  common.ValueTypeUint32Array,
	common.ValueTypeInt64:   common.ValueTypeInt64Array,
	common.ValueTypeUint64:  common.ValueTypeUint64Array,
	common.ValueTypeFloat32: common.ValueTypeFloat32Array,
	common.ValueTypeFloat64: common.ValueTypeFloat64Array,
	common.ValueTypeBool:    common.ValueTypeBoolArray,
}

// skipTypes are types XRT cannot represent: no resource is emitted, but the
// cursor must still advance by this many bytes. Keyed by the canonical spelling.
var skipTypes = map[string]int{
	// Date and time. LDT is 8 bytes (ns since 1970); the design spec's 12 is DTL's.
	"DT":    8,
	"LTIME": 8,
	"LDT":   8,
	"DTL":   12,

	// The Checklist marks these unsupported. The design spec lists Char as
	// supported, but the Checklist is measured on real hardware and wins.
	"CHAR":  1,
	"WCHAR": 2,

	// System data types, sizes per the Siemens system-data-types table.
	"ERRORSTRUCT": 28,
	"CREF":        8,
	"NREF":        8,

	// Hardware, connection, data-block, event and organisation-block
	// identifiers. All derive from a 2-byte base except CONN_R_ID (DWORD) and
	// AOM_IDENT (4), which the EVENT_ types derive from in turn.
	"HW_ANY": 2, "HW_DEVICE": 2, "HW_DPMASTER": 2, "HW_DPSLAVE": 2,
	"HW_HSC": 2, "HW_IEPORT": 2, "HW_INTERFACE": 2, "HW_IO": 2,
	"HW_IOSYSTEM": 2, "HW_MODULE": 2, "HW_PTO": 2, "HW_PWM": 2,
	"HW_SUBMODULE": 2, "PORT": 2,

	"OB_ANY": 2, "OB_ATT": 2, "OB_CYCLIC": 2, "OB_DELAY": 2, "OB_DIAG": 2,
	"OB_HWINT": 2, "OB_PCYCLE": 2, "OB_STARTUP": 2, "OB_TIMEERROR": 2,
	"OB_TOD": 2,

	"CONN_ANY": 2, "CONN_OUC": 2, "CONN_PRG": 2, "CONN_R_ID": 4,

	"DB_ANY": 2, "DB_DYN": 2, "DB_WWW": 2,

	"AOM_IDENT": 4, "EVENT_ANY": 4, "EVENT_ATT": 4, "EVENT_HWINT": 4,

	"PIP": 2, "RTM": 2,
}

// iecTimerNames are the IEC timer types. TON/TOF/TP/TONR are the timer function
// blocks; a declaration may name the block or the IEC_TIMER type, and all share
// one layout, so they take the same size and attributes.
var iecTimerNames = map[string]bool{
	"TON": true, "TOF": true, "TP": true, "TONR": true, "IEC_TIMER": true,
}

// iecTimerSize is the Siemens system-data-types figure for IEC_TIMER.
const iecTimerSize = 16

type iecCounterInfo struct {
	// counterType is the XRT attribute value, not an EdgeX valueType: a counter's
	// valueType is always Object, and these are lower-case.
	counterType string
	byteSize    int
}

// iecCounterTypes sizes are the Siemens system-data-types figures: six BOOL flags
// padded to the value width, plus PV and CV.
//
// The 64-bit counters are absent deliberately: XRT's counterType attribute has no
// 64-bit value, so they are rejected rather than emitted.
var iecCounterTypes = map[string]iecCounterInfo{
	"IEC_SCOUNTER":  {"int8", 3},
	"IEC_USCOUNTER": {"uint8", 3},
	"IEC_COUNTER":   {"int16", 6},
	"IEC_UCOUNTER":  {"uint16", 6},
	"IEC_DCOUNTER":  {"int32", 12},
	"IEC_UDCOUNTER": {"uint32", 12},
}

// S7 device-resource attribute keys and type values, per the XRT S7
// device-service spec:
// https://docs.iotechsys.com/edge-connect33/xrt/devices/s7.html
//
// A resource's kind is told apart by its "type" attribute:
//
//	data block   : type DB          + DB_number / start (+ bitIndex / size / array_size)
//	IEC timer    : type IEC_Timer   + DB_number / start
//	IEC counter  : type IEC_Counter + DB_number / start / counterType
//
// The spec also defines IPU / IPI (process image), Timer / Counter (classic S7
// timers, which take timeBase) and PLC / MISC (status and diagnostics, which
// take an operation). None are produced here: a DB source file describes data
// block contents only, so there is no source in the input for them.
const (
	// attribute keys
	attrType        = "type"
	attrDBNumber    = "DB_number"
	attrStart       = "start"
	attrBitIndex    = "bitIndex"
	attrSize        = "size"
	attrArraySize   = "array_size"
	attrCounterType = "counterType"

	// "type" attribute values
	typeDB         = "DB"
	typeIECTimer   = "IEC_Timer"
	typeIECCounter = "IEC_Counter"
)
