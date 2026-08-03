// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// Convert must wrap a parse failure as an EdgeX error rather than panicking or
// swallowing it.
func TestConvertMalformedInputErrors(t *testing.T) {
	lc := logger.NewMockClient()
	// Unterminated statement (no ";") -> parse returns KindContractInvalid.
	_, err := Convert(context.Background(), lc, []byte("[Device]\n    Name = \"x\"\n"), nil)
	if err == nil {
		t.Fatal("expected error for malformed EDS")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// convertSample converts the golden EDS and asserts it passes EdgeX validation,
// returning the profile for the focused sub-tests below.
func convertSample(t *testing.T) dtos.DeviceProfile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "ethernetip-sample.eds"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	profile, cerr := Convert(context.Background(), logger.NewMockClient(), data, nil)
	if cerr != nil {
		t.Fatalf("Convert on valid EDS: %v", cerr)
	}
	if verr := dtos.ValidateDeviceProfileDTO(profile); verr != nil {
		t.Fatalf("validation: %v", verr)
	}
	return profile
}

// Convert on a well-formed EDS produces a validated profile carrying the device
// identity and its I/O resources. Each aspect is a focused sub-test so a failure
// pinpoints the broken part of the pipeline.
func TestConvertValidInputSucceeds(t *testing.T) {
	profile := convertSample(t)
	rs := profile.DeviceResources

	t.Run("identity", func(t *testing.T) {
		if profile.Name != "ethernetip-sample" {
			t.Errorf("profile Name: got %q, want ethernetip-sample", profile.Name)
		}
		if profile.Manufacturer != "Coverage Test Vendor" {
			t.Errorf("profile Manufacturer: got %q", profile.Manufacturer)
		}
	})

	t.Run("resource counts", func(t *testing.T) { assertResourceKinds(t, rs) })
	t.Run("pads not leaked", func(t *testing.T) { assertPadsNotLeaked(t, rs) })
	t.Run("io spot-check", func(t *testing.T) { assertIOResources(t, rs) })
	t.Run("scaling", func(t *testing.T) { assertScaling(t, rs) })
	t.Run("settings", func(t *testing.T) { assertSettings(t, rs) })
	t.Run("explicit", func(t *testing.T) { assertExplicit(t, rs) })
	t.Run("enum command", func(t *testing.T) { assertEnumCommand(t, profile) })
}

// Enum9 -> Output Mode becomes a DeviceCommand with resourceOperation.mappings.
func assertEnumCommand(t *testing.T, profile dtos.DeviceProfile) {
	t.Helper()
	if len(profile.DeviceCommands) != 1 {
		t.Fatalf("device commands: got %d, want 1 (Output Mode enum)", len(profile.DeviceCommands))
	}
	cmd := profile.DeviceCommands[0]
	if cmd.Name != "Output Mode" || cmd.ReadWrite != common.ReadWrite_W {
		t.Errorf("command: got name=%q rw=%q, want Output Mode/W", cmd.Name, cmd.ReadWrite)
	}
	if len(cmd.ResourceOperations) != 1 || cmd.ResourceOperations[0].DeviceResource != "Output Mode" {
		t.Fatalf("resourceOperations: got %+v", cmd.ResourceOperations)
	}
	want := map[string]string{"0": "Idle", "1": "Run", "2": "Jog", "3": "Home"}
	got := cmd.ResourceOperations[0].Mappings
	if len(got) != len(want) {
		t.Fatalf("mappings: got %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("mappings[%s]: got %q, want %q", k, got[k], v)
		}
	}
}

// Total count and per-kind breakdown: 9 O2T I/O + 7 T2O I/O + 3 Settings +
// 4 explicit = 23. Pads (2) and the config assembly's members produce none.
// Locking the total catches a builder that drops or duplicates resources.
func assertResourceKinds(t *testing.T, rs []dtos.DeviceResource) {
	t.Helper()
	if len(rs) != 23 {
		t.Fatalf("total device resources: got %d, want 23", len(rs))
	}
	var io, settings, explicit int
	for _, r := range rs {
		typ, _ := r.Attributes[attrType].(string)
		switch typ {
		case typeO2T, typeT2O:
			io++
		case typeO2TSettings, typeT2OSettings, typeConfigSettings:
			settings++
		case "":
			explicit++
		}
	}
	if io != 16 || settings != 3 || explicit != 4 {
		t.Errorf("resource kinds: io=%d settings=%d explicit=%d, want 16/3/4", io, settings, explicit)
	}
}

// Pad members must not surface as resources.
func assertPadsNotLeaked(t *testing.T, rs []dtos.DeviceResource) {
	t.Helper()
	for _, name := range []string{"RESERVED_pad5", "RESERVED_pad8"} {
		for _, r := range rs {
			if r.Name == name {
				t.Errorf("pad member %q leaked into the profile", name)
			}
		}
	}
}

// Spot-check representative resources across the pipeline, confirming each
// builder's output reaches the final profile with the right values (per-value
// golden assertions live in the mapper unit tests; this checks the wiring).
func assertIOResources(t *testing.T, rs []dtos.DeviceResource) {
	t.Helper()
	type want struct {
		typ                  string
		offBytes             int
		valueType, readWrite string
	}
	cases := map[string]want{
		"DO Channel 0": {typeO2T, 0, common.ValueTypeBool, common.ReadWrite_W},    // BOOL bit-packed at byte0
		"Output SINT":  {typeO2T, 1, common.ValueTypeInt8, common.ReadWrite_W},    // after 3 bits + 5-bit pad
		"Output REAL":  {typeO2T, 8, common.ValueTypeFloat32, common.ReadWrite_W}, // 4-byte aligned
		"Input LINT":   {typeT2O, 0, common.ValueTypeInt64, common.ReadWrite_R},   // T2O read side
		"Input WORD":   {typeT2O, 26, common.ValueTypeUint16, common.ReadWrite_R}, // after an 8-bit pad
		"Setpoint":     {typeO2T, 14, common.ValueTypeInt16, common.ReadWrite_W},  // O2T side of the shared point
		"Setpoint_T2O": {typeT2O, 32, common.ValueTypeInt16, common.ReadWrite_R},  // T2O side, direction-suffixed
	}
	for name, w := range cases {
		r := resourceByName(t, rs, name)
		if r.Attributes[attrType] != w.typ || r.Attributes[attrOffsetBytes] != w.offBytes {
			t.Errorf("%s: got type=%v byte=%v, want %s/%d", name, r.Attributes[attrType], r.Attributes[attrOffsetBytes], w.typ, w.offBytes)
		}
		if r.Properties.ValueType != w.valueType || r.Properties.ReadWrite != w.readWrite {
			t.Errorf("%s: got %s/%s, want %s/%s", name, r.Properties.ValueType, r.Properties.ReadWrite, w.valueType, w.readWrite)
		}
	}
}

// Scaling reaches the profile: Output INT scaled has scale 0.1 and min -3200.
func assertScaling(t *testing.T, rs []dtos.DeviceResource) {
	t.Helper()
	scaled := resourceByName(t, rs, "Output INT scaled")
	if scaled.Properties.Scale == nil || *scaled.Properties.Scale != 0.1 {
		t.Errorf("Output INT scaled scale: got %v, want 0.1", scaled.Properties.Scale)
	}
	if scaled.Properties.Minimum == nil || *scaled.Properties.Minimum != -3200 {
		t.Errorf("Output INT scaled minimum: got %v, want -3200", scaled.Properties.Minimum)
	}
}

// Settings: O2T (id 100, header true), T2O (id 101, header false), Config
// (id 110, no header) — the includeHeader32bit rule per settings type.
func assertSettings(t *testing.T, rs []dtos.DeviceResource) {
	t.Helper()
	o2tSet := resourceByName(t, rs, "Output Assembly O2TSettings")
	if o2tSet.Attributes[attrAssemblyID] != 100 || o2tSet.Attributes[attrIncludeHeader32bit] != true {
		t.Errorf("O2TSettings: got id=%v hdr=%v, want 100/true", o2tSet.Attributes[attrAssemblyID], o2tSet.Attributes[attrIncludeHeader32bit])
	}
	t2oSet := resourceByName(t, rs, "Input Assembly T2OSettings")
	if t2oSet.Attributes[attrAssemblyID] != 101 || t2oSet.Attributes[attrIncludeHeader32bit] != false {
		t.Errorf("T2OSettings: got id=%v hdr=%v, want 101/false", t2oSet.Attributes[attrAssemblyID], t2oSet.Attributes[attrIncludeHeader32bit])
	}
	cfgSet := resourceByName(t, rs, "Config Assembly ConfigSettings")
	if cfgSet.Attributes[attrAssemblyID] != 110 {
		t.Errorf("ConfigSettings assemblyID: got %v, want 110", cfgSet.Attributes[attrAssemblyID])
	}
	if _, ok := cfgSet.Attributes[attrIncludeHeader32bit]; ok {
		t.Error("ConfigSettings must not carry includeHeader32bit")
	}
}

// Explicit: SerialNumber (Identity/1/6, no type) and Short Label (vendor
// object class 0x67=103) — Link Path EPATH decoded into objClass/instID/attrID.
func assertExplicit(t *testing.T, rs []dtos.DeviceResource) {
	t.Helper()
	sn := resourceByName(t, rs, "SerialNumber (explicit)")
	if _, hasType := sn.Attributes[attrType]; hasType {
		t.Error("explicit resource must not have a type attribute")
	}
	if sn.Attributes[attrObjClass] != 1 || sn.Attributes[attrInstID] != 1 || sn.Attributes[attrAttrID] != 6 {
		t.Errorf("SerialNumber: got %v, want objClass1/inst1/attr6", sn.Attributes)
	}
	if label := resourceByName(t, rs, "Short Label"); label.Attributes[attrObjClass] != 103 {
		t.Errorf("Short Label objClass: got %v, want 103", label.Attributes[attrObjClass])
	}
}

// A [Params] param with a Link Path becomes an explicit-messaging resource with
// no dependence on a [Connection Manager]: an EDS with no connection at all still
// converts, as long as it has such a param. (Implicit I/O and settings need a
// connection; explicit messaging does not.)
func TestConvertExplicitOnlyNoConnection(t *testing.T) {
	eds := []byte("[Device]\n" +
		"    ProdName = \"Explicit Only\";\n" +
		"[Params]\n" +
		"    Param1 = 0, 6, \"20 01 24 01 30 06\", 0x0000, 0xC8,4, \"SerialNumber\";\n")
	profile, err := Convert(context.Background(), logger.NewMockClient(), eds, nil)
	if err != nil {
		t.Fatalf("Convert on explicit-only EDS: %v", err)
	}
	if verr := dtos.ValidateDeviceProfileDTO(profile); verr != nil {
		t.Fatalf("validation: %v", verr)
	}
	if len(profile.DeviceResources) != 1 {
		t.Fatalf("resources: got %d, want 1 (the explicit param)", len(profile.DeviceResources))
	}
	r := profile.DeviceResources[0]
	if _, hasType := r.Attributes[attrType]; hasType {
		t.Error("explicit resource must not have a type attribute")
	}
	if r.Attributes[attrObjClass] != 1 || r.Attributes[attrInstID] != 1 || r.Attributes[attrAttrID] != 6 {
		t.Errorf("explicit attrs: got %v, want objClass1/inst1/attr6", r.Attributes)
	}
}

// Input with no convertible content is a contract error, not a silent empty
// profile. Three shapes must all be rejected: empty input, a [Params] section
// with no [Device] name to derive from, and a named [Device] that yields no
// resources at all (nothing for a caller to distinguish from a real result).
func TestConvertNoConvertibleContentErrors(t *testing.T) {
	cases := map[string][]byte{
		"empty":           nil,
		"no device":       []byte("[Params]\n    Param1 = 0,,,0x0000,0xC1,1,\"X\";\n"),
		"named but empty": []byte("[Device]\n    VendCode = 1;\n    ProdName = \"WidgetX\";\n"),
	}
	for name, data := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(), data, nil)
			if err == nil {
				t.Fatal("expected error for input with no convertible content")
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}
}
