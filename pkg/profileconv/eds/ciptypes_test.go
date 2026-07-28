// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func TestValueTypeForCIP(t *testing.T) {
	tests := []struct {
		code string
		want string
	}{
		{"0xC1", common.ValueTypeBool},
		{"0xC2", common.ValueTypeInt8},
		{"0xC3", common.ValueTypeInt16},
		{"0xC4", common.ValueTypeInt32},
		{"0xC5", common.ValueTypeInt64},
		{"0xC6", common.ValueTypeUint8},
		{"0xC7", common.ValueTypeUint16},
		{"0xC8", common.ValueTypeUint32},
		{"0xC9", common.ValueTypeUint64},
		{"0xCA", common.ValueTypeFloat32},
		{"0xCB", common.ValueTypeFloat64},
		{"0xD0", common.ValueTypeString},
		{"0xDA", common.ValueTypeString},
		{"0xD1", common.ValueTypeUint8},
		{"0xD2", common.ValueTypeUint16},
		{"0xD3", common.ValueTypeUint32},
		{"0xD4", common.ValueTypeUint64},
		// Same code in decimal and lower-case hex must resolve identically.
		{"195", common.ValueTypeInt16}, // 0xC3
		{"0xc3", common.ValueTypeInt16},
	}
	for _, tt := range tests {
		t.Run(tt.code, func(t *testing.T) {
			got, err := valueTypeForCIP(tt.code)
			if err != nil {
				t.Fatalf("valueTypeForCIP(%q): %v", tt.code, err)
			}
			if got != tt.want {
				t.Errorf("valueTypeForCIP(%q): got %q, want %q", tt.code, got, tt.want)
			}
		})
	}
}

func TestValueTypeForCIPUnsupported(t *testing.T) {
	// Unsupported codes must error rather than silently produce a wrong
	// valueType: EPATH (0xDC), a time type (0xC0), and — importantly — the
	// string variants STRING2/STRINGN/STRINGI (0xD5/0xD9/0xDE), which are NOT
	// mapped to String because their encodings differ. Plus bad literals.
	for _, code := range []string{"0xDC", "0xC0", "0xD5", "0xD9", "0xDE", "0xDD", "0xFF", "", "nothex"} {
		t.Run(code, func(t *testing.T) {
			_, err := valueTypeForCIP(code)
			if err == nil {
				t.Fatalf("valueTypeForCIP(%q): expected error", code)
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}
}

// #6: the shared normalizer must fold the mixed casing seen in real profiles
// (ethernetip-sim-profile.json has both "Uint8array" and "Uint8Array") to the
// spelling the EdgeX validator accepts. We rely on common.NormalizeValueType
// rather than a hand-rolled map; this test pins that reliance.
func TestNormalizeValueTypeCasing(t *testing.T) {
	for _, in := range []string{"Uint8array", "Uint8Array", "uint8array", "UINT8ARRAY"} {
		got, err := common.NormalizeValueType(in)
		if err != nil {
			t.Fatalf("NormalizeValueType(%q): %v", in, err)
		}
		if got != common.ValueTypeUint8Array {
			t.Errorf("NormalizeValueType(%q): got %q, want %q", in, got, common.ValueTypeUint8Array)
		}
	}
}
