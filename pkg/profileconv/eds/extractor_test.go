// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func extractSample(t *testing.T) *extracted {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "ethernetip-sample.eds"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	e, perr := parse(bytes.NewReader(data))
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	x, xerr := extract(logger.NewMockClient(), e)
	if xerr != nil {
		t.Fatalf("extract: %v", xerr)
	}
	return x
}

func TestExtractDevice(t *testing.T) {
	x := extractSample(t)
	if x.device.vendorName != "Coverage Test Vendor" {
		t.Errorf("vendorName: got %q", x.device.vendorName)
	}
	if x.device.productName != "EtherNetIP Sample" {
		t.Errorf("productName: got %q", x.device.productName)
	}
}

func TestExtractParams(t *testing.T) {
	x := extractSample(t)
	// Param6 is the INT with scaling: name, type, units, scale mult/div.
	p, ok := x.params["Param6"]
	if !ok {
		t.Fatal("Param6 missing from lookup")
	}
	if p.name != "Output INT scaled" {
		t.Errorf("Param6 name: got %q", p.name)
	}
	if p.dataType != "0xC3" {
		t.Errorf("Param6 dataType: got %q, want 0xC3", p.dataType)
	}
	if p.units != "deg" {
		t.Errorf("Param6 units: got %q, want deg", p.units)
	}
	if p.scaleMult != "1" || p.scaleDiv != "10" {
		t.Errorf("Param6 scale: got mult=%q div=%q, want 1/10", p.scaleMult, p.scaleDiv)
	}
	// Param22 is explicit (has a Link Path).
	if x.params["Param22"].linkPath == "" {
		t.Error("Param22 should have a Link Path (explicit param)")
	}
	// Param1 is implicit (no Link Path).
	if x.params["Param1"].linkPath != "" {
		t.Errorf("Param1 should have no Link Path, got %q", x.params["Param1"].linkPath)
	}
}

// Enum9 (value->label) must attach to Param9 by matching N; a param without an
// EnumN carries no enum.
func TestExtractParamEnum(t *testing.T) {
	x := extractSample(t)
	got := x.params["Param9"].enum
	want := map[string]string{"0": "Idle", "1": "Run", "2": "Jog", "3": "Home"}
	if len(got) != len(want) {
		t.Fatalf("Param9 enum: got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Param9 enum[%s]: got %q, want %q", k, got[k], v)
		}
	}
	if x.params["Param1"].enum != nil {
		t.Errorf("Param1 should have no enum, got %v", x.params["Param1"].enum)
	}
}

// attachEnums robustness: an EnumN with no matching ParamN is dropped (no panic),
// an odd field count drops the trailing unpaired value, and an empty value is
// skipped — the remaining clean pairs still attach.
func TestExtractParamEnumEdgeCases(t *testing.T) {
	src := "[Params]\n" +
		"    Param1 = 0,,,0x0000,0xC7,2,\"Mode\",\"\",\"help\";\n" +
		"    Enum1 = 0,\"Idle\", 1,\"Run\", 2;\n" + // odd: trailing "2" has no label
		"    Enum7 = 0,\"Orphan\";\n" // no Param7 -> dropped
	e, perr := parse(bytes.NewReader([]byte(src)))
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	x, err := extract(logger.NewMockClient(), e)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	// Param1 keeps only the two complete pairs; the trailing "2" is dropped.
	got := x.params["Param1"].enum
	want := map[string]string{"0": "Idle", "1": "Run"}
	if len(got) != len(want) {
		t.Fatalf("Param1 enum: got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("Param1 enum[%s]: got %q, want %q", k, got[k], v)
		}
	}
	// Enum7 had no Param7 -> nothing created, no panic.
	if _, ok := x.params["Param7"]; ok {
		t.Error("orphan Enum7 should not create a Param7")
	}
}

func TestExtractAssembly(t *testing.T) {
	x := extractSample(t)
	a, ok := x.assemblies["Assem100"]
	if !ok {
		t.Fatal("Assem100 missing")
	}
	if a.assemblyID != 100 { // 0x64
		t.Errorf("Assem100 id: got %d, want 100", a.assemblyID)
	}
	if a.size != "16" {
		t.Errorf("Assem100 size: got %q, want 16", a.size)
	}
	// Assem100 has 10 members: DO0/DO1/DO2 (1 bit), a 5-bit pad, then SINT/INT/
	// DINT/REAL/UINT/Setpoint. Lock the exact count so a misalignment that
	// adds or drops a member is caught.
	wantRefs := []string{"Param1", "Param2", "Param3", "Param4", "Param5", "Param6", "Param7", "Param8", "Param9", "Param24"}
	if len(a.members) != len(wantRefs) {
		t.Fatalf("Assem100 members: got %d %+v, want %d", len(a.members), a.members, len(wantRefs))
	}
	for i, want := range wantRefs {
		if a.members[i].paramRef != want {
			t.Errorf("member %d ref: got %q, want %q", i, a.members[i].paramRef, want)
		}
	}
	// member[3] is the pad: 5 bits, referencing the RESERVED param (kept, not
	// dropped — the mapper decides pads produce no resource).
	if a.members[0].bitLength != "1" || a.members[3].bitLength != "5" {
		t.Errorf("member bit lengths: got [0]=%q [3]=%q, want 1 and 5", a.members[0].bitLength, a.members[3].bitLength)
	}
	if a.members[3].paramRef != "Param4" {
		t.Errorf("pad member[3] ref: got %q, want Param4", a.members[3].paramRef)
	}
}

// Assem101 uses the empty-size member style (", Param10" — bit length omitted,
// taken from the param's Data Size later). Members must still be captured with
// an empty bitLength, not dropped.
func TestExtractAssemblyEmptySizeMembers(t *testing.T) {
	a := extractSample(t).assemblies["Assem101"]
	if a.assemblyID != 101 { // 0x65
		t.Fatalf("Assem101 id: got %d, want 101", a.assemblyID)
	}
	if len(a.members) == 0 {
		t.Fatal("Assem101 members dropped (empty-size style not handled)")
	}
	m0 := a.members[0]
	if m0.paramRef != "Param10" || m0.bitLength != "" {
		t.Errorf("Assem101 member[0]: got bits=%q ref=%q, want bits=\"\" ref=Param10", m0.bitLength, m0.paramRef)
	}
}

func TestExtractConnection(t *testing.T) {
	x := extractSample(t)
	if len(x.connections) != 1 {
		t.Fatalf("connections: got %d, want 1", len(x.connections))
	}
	c := x.connections[0]
	if c.o2tFormat != "Assem100" {
		t.Errorf("o2tFormat: got %q, want Assem100", c.o2tFormat)
	}
	if c.t2oFormat != "Assem101" {
		t.Errorf("t2oFormat: got %q, want Assem101", c.t2oFormat)
	}
	if c.configFormat != "Assem110" {
		t.Errorf("configFormat: got %q, want Assem110", c.configFormat)
	}
	// field 1 drives includeHeader32bit later; it is the most off-by-one-prone
	// field, so pin its value.
	if c.connectionParams != "0x44640405" {
		t.Errorf("connectionParams: got %q, want 0x44640405", c.connectionParams)
	}
}

func TestAssemblyIDFromPath(t *testing.T) {
	tests := []struct {
		path string
		want int
	}{
		{"20 04 24 64 30 03", 100},       // 0x64 8-bit instance
		{"20 04 24 65 30 03", 101},       // 0x65
		{"20 04 24 6E 30 03", 110},       // 0x6E
		{`"20 04 24 64 30 03"`, 100},     // quoted as it appears in EDS
		{"20 04 2C 64", 100},             // 8-bit connection point
		{"20 04 2D 00 00 01", 256},       // 16-bit connection point: pad 00, then 00 01 LE
		{"20 04 25 00 00 01 30 03", 256}, // 16-bit instance (id >= 256): pad 00, then 00 01 LE
		// class value 0x24 must NOT be read as an instance segment; the
		// pairwise walk skips the (0x20, 0x24) class pair and reads (0x24, 0x64).
		{"20 24 24 64 30 03", 100},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			got, err := assemblyIDFromPath(tt.path)
			if err != nil {
				t.Fatalf("assemblyIDFromPath(%q): %v", tt.path, err)
			}
			if got != tt.want {
				t.Errorf("got %d, want %d", got, tt.want)
			}
		})
	}
}

func TestAssemblyIDFromPathErrors(t *testing.T) {
	// "20 04 27 ..." : 0x27 = instance segment with reserved size format (bits=3).
	// "20 04 25 FF 00 01" : 16-bit instance whose pad byte is non-zero (must be 0x00).
	for _, path := range []string{"", "20 04 30 03", "nothex", "20 04 27 01 02", "20 04 25 FF 00 01"} {
		t.Run(path, func(t *testing.T) {
			_, err := assemblyIDFromPath(path)
			if err == nil {
				t.Fatalf("expected error for %q", path)
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}
}

// A blank assembly Path (a dynamic/placeholder assembly, common in modular
// devices) is skipped rather than causing extract to fail. A malformed non-blank
// Path is still an error.
func TestExtractSkipsBlankPathAssembly(t *testing.T) {
	src := "[Assembly]\n" +
		"    Assem1 = \"Fixed\", \"20 04 24 64 30 03\", 2, 0x0, , , 8, Param1;\n" +
		"    Assem2 = \"Dynamic\", , 0, 0x0, , ;\n" // blank Path
	e, perr := parse(bytes.NewReader([]byte(src)))
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	x, err := extract(logger.NewMockClient(), e)
	if err != nil {
		t.Fatalf("extract should skip blank-path assembly, not error: %v", err)
	}
	if _, ok := x.assemblies["Assem1"]; !ok {
		t.Error("fixed assembly with a valid path should be kept")
	}
	if _, ok := x.assemblies["Assem2"]; ok {
		t.Error("blank-path assembly should be skipped")
	}
}

// A repeated section (e.g. two [Params] blocks) must have all its entries read,
// not just the first block's — otherwise later params silently vanish.
func TestExtractMergesRepeatedSection(t *testing.T) {
	src := "[Params]\n" +
		"    Param1 = 0,,,0x0000,0xC1,1,\"A\";\n" +
		"[Params]\n" +
		"    Param2 = 0,,,0x0000,0xC1,1,\"B\";\n"
	e, perr := parse(bytes.NewReader([]byte(src)))
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	x, err := extract(logger.NewMockClient(), e)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if _, ok := x.params["Param1"]; !ok {
		t.Error("Param1 (first [Params] block) missing")
	}
	if _, ok := x.params["Param2"]; !ok {
		t.Error("Param2 (second [Params] block) dropped — repeated section not merged")
	}
}
