// Copyright (C) 2026 IOTech Ltd

package tia

import (
	"strings"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
)

// The type tables decide both the emitted valueType and how far the offset
// cursor advances, so a wrong entry misaligns every resource after it. These
// tests pin the values that came from the XRT Checklist, a working XRT profile
// and the Siemens docs — the numbers most likely to be "corrected" by mistake.

// Every scalar valueType must be an EdgeX constant, or dtos.DeviceProfile.Validate
// rejects the whole profile. A bare lower-case string would pass compilation and
// fail only at runtime.
func TestScalarTypesUseValidEdgeXValueTypes(t *testing.T) {
	valid := map[string]bool{
		common.ValueTypeBool: true, common.ValueTypeString: true, common.ValueTypeObject: true,
		common.ValueTypeInt8: true, common.ValueTypeInt16: true,
		common.ValueTypeInt32: true, common.ValueTypeInt64: true,
		common.ValueTypeUint8: true, common.ValueTypeUint16: true,
		common.ValueTypeUint32: true, common.ValueTypeUint64: true,
		common.ValueTypeFloat32: true, common.ValueTypeFloat64: true,
	}
	for s7, info := range scalarTypes {
		if !valid[info.valueType] {
			t.Errorf("%s maps to %q, which is not an EdgeX value-type constant", s7, info.valueType)
		}
	}
}

// Sizes, signedness and alignment, per the Siemens docs. Two entries matter most:
// Bool is bit-packed so its byteSize is 0 and the tracker handles it, and the
// 1-byte types must declare alignment 0 — declaring 2 would push them onto even
// addresses and silently shift everything downstream.
func TestScalarTypeSizes(t *testing.T) {
	tests := []struct {
		s7        string
		valueType string
		size      int
		align     int
	}{
		{"BOOL", common.ValueTypeBool, 0, 0},
		{"SINT", common.ValueTypeInt8, 1, 0},
		{"USINT", common.ValueTypeUint8, 1, 0},
		{"BYTE", common.ValueTypeUint8, 1, 0},
		{"INT", common.ValueTypeInt16, 2, 2},
		{"UINT", common.ValueTypeUint16, 2, 2},
		{"WORD", common.ValueTypeUint16, 2, 2},
		{"DINT", common.ValueTypeInt32, 4, 2},
		{"UDINT", common.ValueTypeUint32, 4, 2},
		{"DWORD", common.ValueTypeUint32, 4, 2},
		{"LINT", common.ValueTypeInt64, 8, 2},
		{"ULINT", common.ValueTypeUint64, 8, 2},
		{"LWORD", common.ValueTypeUint64, 8, 2},
		{"REAL", common.ValueTypeFloat32, 4, 2},
		{"LREAL", common.ValueTypeFloat64, 8, 2},
		// Time is a signed interval (+-24.8 days) and does use the full Int32 range.
		{"TIME", common.ValueTypeInt32, 4, 2},
		{"DATE", common.ValueTypeUint16, 2, 2},
		{"TOD", common.ValueTypeUint32, 4, 2},
		{"LTOD", common.ValueTypeUint64, 8, 2},
	}
	// Every entry must be listed: an unlisted type's size and alignment would go
	// unchecked, and a wrong size misaligns every later address.
	if len(tests) != len(scalarTypes) {
		t.Errorf("table covers %d of %d scalarTypes entries", len(tests), len(scalarTypes))
	}
	for _, tt := range tests {
		t.Run(tt.s7, func(t *testing.T) {
			info, ok := scalarTypes[tt.s7]
			if !ok {
				t.Fatalf("%s missing from scalarTypes", tt.s7)
			}
			if info.valueType != tt.valueType {
				t.Errorf("valueType: got %q, want %q", info.valueType, tt.valueType)
			}
			if info.byteSize != tt.size {
				t.Errorf("byteSize: got %d, want %d", info.byteSize, tt.size)
			}
			if info.alignment != tt.align {
				t.Errorf("alignment: got %d, want %d", info.alignment, tt.align)
			}
		})
	}
}

// Array valueTypes carry the element bit width, matching the working XRT profile
// (UInt8Array / Int64Array / StringArray) rather than the width-less "Int Array"
// spelling in the user docs. Every scalar array type must also be a valid EdgeX
// constant.
func TestArrayTypesMapElementToArrayValueType(t *testing.T) {
	tests := []struct{ elem, want string }{
		{common.ValueTypeBool, common.ValueTypeBoolArray},
		{common.ValueTypeInt8, common.ValueTypeInt8Array},
		{common.ValueTypeUint8, common.ValueTypeUint8Array},
		{common.ValueTypeInt16, common.ValueTypeInt16Array},
		{common.ValueTypeUint16, common.ValueTypeUint16Array},
		{common.ValueTypeInt32, common.ValueTypeInt32Array},
		{common.ValueTypeUint32, common.ValueTypeUint32Array},
		{common.ValueTypeInt64, common.ValueTypeInt64Array},
		{common.ValueTypeUint64, common.ValueTypeUint64Array},
		{common.ValueTypeFloat32, common.ValueTypeFloat32Array},
		{common.ValueTypeFloat64, common.ValueTypeFloat64Array},
	}
	for _, tt := range tests {
		t.Run(tt.elem, func(t *testing.T) {
			if got := arrayTypes[tt.elem]; got != tt.want {
				t.Errorf("array of %s: got %q, want %q", tt.elem, got, tt.want)
			}
		})
	}
}

// Every scalar that can be an array element needs an arrayTypes entry, otherwise
// an Array[..] of that type is silently skipped instead of emitted.
func TestEveryScalarHasAnArrayCounterpart(t *testing.T) {
	for s7, info := range scalarTypes {
		// String is handled separately (it has a per-element size attribute).
		if info.valueType == common.ValueTypeString {
			continue
		}
		if _, ok := arrayTypes[info.valueType]; !ok {
			t.Errorf("%s maps to %s, which has no arrayTypes entry", s7, info.valueType)
		}
	}
}

// IEC counter sizes, per the Siemens system-data-types table: six BOOL flags
// padded to the value width, plus PV and CV. counterType is XRT's attribute value
// and is lower-case, unlike an EdgeX valueType.
func TestIECCounterSizesAndCounterTypes(t *testing.T) {
	tests := []struct {
		s7          string
		counterType string
		size        int
	}{
		{"IEC_SCOUNTER", "int8", 3},
		{"IEC_USCOUNTER", "uint8", 3},
		{"IEC_COUNTER", "int16", 6},
		{"IEC_UCOUNTER", "uint16", 6},
		{"IEC_DCOUNTER", "int32", 12},
		{"IEC_UDCOUNTER", "uint32", 12},
	}
	for _, tt := range tests {
		t.Run(tt.s7, func(t *testing.T) {
			info, ok := iecCounterTypes[tt.s7]
			if !ok {
				t.Fatalf("%s missing from iecCounterTypes", tt.s7)
			}
			if info.byteSize != tt.size {
				t.Errorf("byteSize: got %d, want %d", info.byteSize, tt.size)
			}
			if info.counterType != tt.counterType {
				t.Errorf("counterType: got %q, want %q", info.counterType, tt.counterType)
			}
		})
	}
}

// IEC_TIMER is 16 bytes per the Siemens system-data-types table.
func TestIECTimerSize(t *testing.T) {
	if iecTimerSize != 16 {
		t.Errorf("iecTimerSize: got %d, want 16", iecTimerSize)
	}
	// The IEC 61131-3 timer instruction names all resolve to the same layout.
	for _, name := range []string{"TON", "TOF", "TP", "TONR", "IEC_TIMER"} {
		if !iecTimerNames[name] {
			t.Errorf("%s not recognised as an IEC timer", name)
		}
	}
}

// Types XRT cannot represent still need a size: the converter emits no resource
// but must advance the cursor, or every later address is wrong. The table is
// keyed by the canonical spelling, so an alias has to be normalised first.
func TestSkipTypeSizes(t *testing.T) {
	tests := []struct {
		s7   string
		size int
	}{
		{"DT", 8},
		{"LTIME", 8},
		{"LDT", 8}, // 8, not the design spec's 12 — that is DTL's size
		{"DTL", 12},
		{"CHAR", 1},
		{"WCHAR", 2},
		{"ERRORSTRUCT", 28},
		{"CREF", 8},
		{"NREF", 8},
		{"HW_DEVICE", 2},
		{"CONN_R_ID", 4}, // DWORD, unlike the other CONN_ types
		{"AOM_IDENT", 4}, // and the EVENT_ types that derive from it
		{"EVENT_ANY", 4},
		{"OB_CYCLIC", 2},
		{"PIP", 2},
	}
	// The identifier families derive from a 2-byte base, except the four listed
	// above. Checking every remaining key against that rule stops a new entry
	// from being added with an unchecked size.
	exceptions := map[string]int{"CONN_R_ID": 4, "AOM_IDENT": 4,
		"EVENT_ANY": 4, "EVENT_ATT": 4, "EVENT_HWINT": 4}
	for name, size := range skipTypes {
		if !isIdentifierFamily(name) {
			continue
		}
		want, ok := exceptions[name]
		if !ok {
			want = 2
		}
		if size != want {
			t.Errorf("%s: size %d, want %d", name, size, want)
		}
	}
	for _, tt := range tests {
		t.Run(tt.s7, func(t *testing.T) {
			got, ok := skipTypes[tt.s7]
			if !ok {
				t.Fatalf("%s missing from skipTypes", tt.s7)
			}
			if got != tt.size {
				t.Errorf("size: got %d, want %d", got, tt.size)
			}
		})
	}
}

// The long and short IEC spellings name the same type, so they must resolve to
// one canonical key. Before this, TOD emitted a resource while TIME_OF_DAY was
// skipped, and LTIME_OF_DAY was not recognised at all — leaving the offset cursor
// stranded and every later address wrong.
func TestNormaliseTypeResolvesAliases(t *testing.T) {
	tests := []struct{ in, want string }{
		{"TIME_OF_DAY", "TOD"},
		{"LTIME_OF_DAY", "LTOD"},
		{"DATE_AND_TIME", "DT"},
		{"DATE_AND_LTIME", "LDT"}, // Siemens spelling
		{"LDATE_AND_TIME", "LDT"}, // IEC spelling
		// A canonical name passes through unchanged.
		{"INT", "INT"},
		{"TOD", "TOD"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := normaliseType(tt.in); got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

// Every alias must land on a key some table actually has, or normalising sends
// the type into the unknown branch instead of its handler.
func TestAliasTargetsExistInATable(t *testing.T) {
	for alias, canonical := range typeAliases {
		_, scalar := scalarTypes[canonical]
		_, skip := skipTypes[canonical]
		if !scalar && !skip {
			t.Errorf("%s normalises to %s, which is in no table", alias, canonical)
		}
	}
}

// The dispatch tables must be pairwise disjoint. emit tries them in a fixed
// order, so a type in two of them takes whichever arm comes first — and the two
// arms do different things (emit a resource versus skip it).
func TestDispatchTablesAreDisjoint(t *testing.T) {
	tables := map[string]map[string]bool{
		"scalarTypes":     keysOf(scalarTypes),
		"skipTypes":       keysOf(skipTypes),
		"iecCounterTypes": keysOf(iecCounterTypes),
		"iecTimerNames":   iecTimerNames,
	}
	names := []string{"scalarTypes", "skipTypes", "iecCounterTypes", "iecTimerNames"}
	for i := range names {
		for j := i + 1; j < len(names); j++ {
			for s7 := range tables[names[i]] {
				if tables[names[j]][s7] {
					t.Errorf("%s is in both %s and %s; emit would silently pick whichever "+
						"arm comes first", s7, names[i], names[j])
				}
			}
		}
	}
}

// keysOf collects a type table's keys so the tables can be compared regardless of
// their value types.
func keysOf[V any](m map[string]V) map[string]bool {
	out := make(map[string]bool, len(m))
	for k := range m {
		out[k] = true
	}
	return out
}

// A maximum is only meaningful where the S7 type's range is narrower than the
// integer carrying it. Publishing one elsewhere would reject values the PLC
// accepts; omitting one where it is needed lets through values it cannot read.
func TestScalarMaximumsMatchTheTypeRange(t *testing.T) {
	tests := []struct {
		s7      string
		maximum float64
	}{
		// Milliseconds and nanoseconds since midnight, both capped just short of
		// the next day.
		{"TOD", 86_399_999},
		{"LTOD", 86_399_999_999_999},
		// Date's range D#1990-01-01..D#2169-06-06 is exactly 0..65535 days, the
		// whole Uint16, so it needs no bound.
		{"DATE", 0},
		// A signed interval that genuinely uses the full Int32 range.
		{"TIME", 0},
		{"INT", 0},
		{"UDINT", 0},
	}
	for _, tt := range tests {
		t.Run(tt.s7, func(t *testing.T) {
			if got := scalarTypes[tt.s7].maximum; got != tt.maximum {
				t.Errorf("maximum: got %v, want %v", got, tt.maximum)
			}
		})
	}
}

// The bounds must survive the round trip through float64, which is what the
// EdgeX DTO uses: an integer above 2^53 would be silently rounded.
func TestScalarMaximumsAreExactAsFloat64(t *testing.T) {
	for s7, info := range scalarTypes {
		if info.maximum == 0 {
			continue
		}
		if info.maximum > 1<<53 {
			t.Errorf("%s maximum %v exceeds the exact-integer range of float64", s7, info.maximum)
		}
	}
}

// isIdentifierFamily reports whether a skipTypes key is one of the hardware,
// organisation-block, connection, data-block or event identifier families, all of
// which derive from a fixed-width base type.
func isIdentifierFamily(name string) bool {
	for _, prefix := range []string{"HW_", "OB_", "CONN_", "DB_", "EVENT_", "AOM_"} {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return name == "PORT" || name == "PIP" || name == "RTM"
}
