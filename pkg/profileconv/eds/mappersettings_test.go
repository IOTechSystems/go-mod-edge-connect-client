// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"strings"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// settingsByType returns the single resource with the given Settings type.
func settingsByType(t *testing.T, rs []dtos.DeviceResource, typ string) dtos.DeviceResource {
	t.Helper()
	for _, r := range rs {
		if r.Attributes[attrType] == typ {
			return r
		}
	}
	t.Fatalf("no resource with type %q", typ)
	return dtos.DeviceResource{}
}

func TestConnectionHeader32bit(t *testing.T) {
	tests := []struct {
		params   string
		o2t, t2o bool
	}{
		// Sample: O2T header format (bits 8-10) = 4 -> true; T2O (bits 12-14) = 0.
		{"0x44640405", true, false},
		{"0x44240405", true, false}, // ATI real EDS: same
		{"0x00004000", false, true}, // T2O (bits 12-14) = 4 only -> t2o true, o2t false
		{"0x00004400", true, true},  // both nibbles = 4 -> both true
		{"", false, false},          // blank -> both false
		{"nothex", false, false},    // unparseable -> both false
	}
	for _, tt := range tests {
		t.Run(tt.params, func(t *testing.T) {
			o2t, t2o := connectionHeader32bit(tt.params)
			if o2t != tt.o2t || t2o != tt.t2o {
				t.Errorf("got o2t=%v t2o=%v, want %v/%v", o2t, t2o, tt.o2t, tt.t2o)
			}
		})
	}
}

func TestMapSettings(t *testing.T) {
	rs, err := extractSample(t).mapSettings(newNameSet())
	if err != nil {
		t.Fatalf("mapSettings: %v", err)
	}

	// One Settings per used assembly: O2T (Assem100), T2O (Assem101), Config
	// (Assem110). Find each by its type attribute.
	byType := map[string]int{}
	for _, r := range rs {
		typ, _ := r.Attributes[attrType].(string)
		byType[typ]++
	}
	for _, typ := range []string{typeO2TSettings, typeT2OSettings, typeConfigSettings} {
		if byType[typ] != 1 {
			t.Errorf("%s: got %d resources, want 1", typ, byType[typ])
		}
	}

	// O2T Settings: assemblyID 100, size 16, includeHeader32bit true (from
	// connection params 0x44640405 bits 8-10 = 4). Placeholder value type/RW.
	o2t := settingsByType(t, rs, typeO2TSettings)
	if o2t.Name != "Output Assembly O2TSettings" {
		t.Errorf("O2T name: got %q, want %q", o2t.Name, "Output Assembly O2TSettings")
	}
	if o2t.Attributes[attrAssemblyID] != 100 {
		t.Errorf("O2T assemblyID: got %v, want 100", o2t.Attributes[attrAssemblyID])
	}
	if o2t.Attributes[attrSize] != 16 {
		t.Errorf("O2T size: got %v, want 16", o2t.Attributes[attrSize])
	}
	if o2t.Attributes[attrIncludeHeader32bit] != true {
		t.Errorf("O2T includeHeader32bit: got %v, want true", o2t.Attributes[attrIncludeHeader32bit])
	}
	if o2t.Properties.ValueType != common.ValueTypeString || o2t.Properties.ReadWrite != common.ReadWrite_R {
		t.Errorf("O2T placeholder props: got %s/%s, want String/R", o2t.Properties.ValueType, o2t.Properties.ReadWrite)
	}

	// T2O Settings: includeHeader32bit false (bits 12-14 = 0).
	t2o := settingsByType(t, rs, typeT2OSettings)
	if t2o.Attributes[attrIncludeHeader32bit] != false {
		t.Errorf("T2O includeHeader32bit: got %v, want false", t2o.Attributes[attrIncludeHeader32bit])
	}

	// Config Settings carries no includeHeader32bit.
	cfg := settingsByType(t, rs, typeConfigSettings)
	if _, ok := cfg.Attributes[attrIncludeHeader32bit]; ok {
		t.Error("ConfigSettings should not have includeHeader32bit")
	}
	if cfg.Attributes[attrAssemblyID] != 110 {
		t.Errorf("Config assemblyID: got %v, want 110", cfg.Attributes[attrAssemblyID])
	}
}

// assertLinkPathAttrs decodes path and checks the objClass/instID and the
// optional attrID (hasAttr=false means the attribute segment must be absent).
func assertLinkPathAttrs(t *testing.T, path string, objClass, instID, attrID int, hasAttr bool) {
	t.Helper()
	attrs, err := explicitAttrsFromLinkPath(path)
	if err != nil {
		t.Fatalf("explicitAttrsFromLinkPath: %v", err)
	}
	if attrs[attrObjClass] != objClass {
		t.Errorf("objClass: got %v, want %d", attrs[attrObjClass], objClass)
	}
	if attrs[attrInstID] != instID {
		t.Errorf("instID: got %v, want %d", attrs[attrInstID], instID)
	}
	if _, ok := attrs[attrAttrID]; ok != hasAttr {
		t.Errorf("attrID present: got %v, want %v", ok, hasAttr)
	}
	if hasAttr && attrs[attrAttrID] != attrID {
		t.Errorf("attrID: got %v, want %d", attrs[attrAttrID], attrID)
	}
}

func TestExplicitAttrsFromLinkPath(t *testing.T) {
	tests := []struct {
		path                     string
		objClass, instID, attrID int
		hasAttr                  bool
	}{
		{"20 01 24 01 30 07", 1, 1, 7, true},    // Identity/1/ProductName
		{"20 67 24 01 30 01", 0x67, 1, 1, true}, // vendor obj 0x67
		{"20 01 24 01", 1, 1, 0, false},         // no attribute segment
		{`"20 01 24 01 30 06"`, 1, 1, 6, true},  // quoted as in EDS
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			assertLinkPathAttrs(t, tt.path, tt.objClass, tt.instID, tt.attrID, tt.hasAttr)
		})
	}
}

func TestExplicitAttrsFromLinkPathErrors(t *testing.T) {
	// All malformed paths must error as KindContractInvalid, not silently
	// truncate or overwrite.
	paths := []string{
		"30 06",             // missing class and instance
		"20 01",             // missing instance
		"nothex hex",        // bad byte
		"",                  // empty
		"20 01 24 01 30",    // odd token count: attribute value missing
		"20 01 20 02 24 01", // duplicate class segment
	}
	for _, path := range paths {
		t.Run(path, func(t *testing.T) {
			_, err := explicitAttrsFromLinkPath(path)
			if err == nil {
				t.Fatalf("expected error for %q", path)
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("kind: got %v", errors.Kind(err))
			}
		})
	}
}

// A 16-bit instance segment (0x25) is now decoded by the shared parseEPATH
// walker, so an explicit Link Path with a class >= 256 works.
func TestExplicitAttrsFromLinkPath16Bit(t *testing.T) {
	attrs, err := explicitAttrsFromLinkPath("21 00 00 01 24 01 30 06") // 16-bit class 0x0100: pad 00, then 00 01 LE
	if err != nil {
		t.Fatalf("explicitAttrsFromLinkPath: %v", err)
	}
	if attrs[attrObjClass] != 256 {
		t.Errorf("objClass: got %v, want 256", attrs[attrObjClass])
	}
}

// A shared assembly used by connections with conflicting includeHeader32bit is
// an error, not a silent first-wins.
func TestMapSettingsConflictingHeaderErrors(t *testing.T) {
	x := &extracted{
		assemblies: map[string]assembly{"A": {name: "Out", assemblyID: 100, size: "2"}},
		connections: []connection{
			{connectionParams: "0x400", o2tFormat: "A"}, // bits 8-10 = 4 -> true
			{connectionParams: "0x0", o2tFormat: "A"},   // -> false
		},
	}
	_, err := x.mapSettings(newNameSet())
	if err == nil {
		t.Fatal("expected error for conflicting includeHeader32bit")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("kind: got %v", errors.Kind(err))
	}
}

func TestMapSettingsSizeAndAssemblyErrors(t *testing.T) {
	// Negative size is rejected.
	neg := &extracted{
		assemblies:  map[string]assembly{"A": {assemblyID: 1, size: "-4"}},
		connections: []connection{{o2tFormat: "A"}},
	}
	if _, err := neg.mapSettings(newNameSet()); err == nil {
		t.Error("expected error for negative assembly size")
	}
	// Connection referencing an unknown assembly is rejected.
	unknown := &extracted{
		assemblies:  map[string]assembly{},
		connections: []connection{{o2tFormat: "Ghost"}},
	}
	if _, err := unknown.mapSettings(newNameSet()); err == nil {
		t.Error("expected error for unknown assembly")
	}
}

// assemblyID and size bounds. Config size cap (400) is tighter than the
// O2T/T2O cap (2000), so a size valid for I/O can be invalid for Config.
func TestMapSettingsValueBounds(t *testing.T) {
	settings := func(dir, size string, id int) errors.EdgeX {
		x := &extracted{
			assemblies: map[string]assembly{"A": {assemblyID: id, size: size}},
		}
		c := connection{}
		switch dir {
		case typeO2TSettings:
			c.o2tFormat = "A"
		case typeConfigSettings:
			c.configFormat = "A"
		}
		x.connections = []connection{c}
		_, err := x.mapSettings(newNameSet())
		return err
	}
	// assemblyID: the cap itself is accepted, one over is rejected (guards use
	// strict ">", so pin both sides against a ">="/off-by-one regression).
	if err := settings(typeO2TSettings, "2", 65535); err != nil {
		t.Errorf("assemblyID 65535 should be valid: %v", err)
	}
	if settings(typeO2TSettings, "2", 65536) == nil {
		t.Error("expected error for assemblyID > 65535")
	}
	// Config size cap (400): exact cap accepted, one over rejected.
	if err := settings(typeConfigSettings, "400", 1); err != nil {
		t.Errorf("Config size 400 should be valid: %v", err)
	}
	if settings(typeConfigSettings, "401", 1) == nil {
		t.Error("expected error for Config size 401 (> 400)")
	}
	// size 500 is fine for O2T (<=2000) but too big for Config (<=400).
	if err := settings(typeO2TSettings, "500", 1); err != nil {
		t.Errorf("O2T size 500 should be valid: %v", err)
	}
	if settings(typeConfigSettings, "500", 1) == nil {
		t.Error("expected error for Config size 500 (> 400)")
	}
	// O2T/T2O size cap (2000): exact cap accepted, one over rejected.
	if err := settings(typeO2TSettings, "2000", 1); err != nil {
		t.Errorf("O2T size 2000 should be valid: %v", err)
	}
	if settings(typeO2TSettings, "2001", 1) == nil {
		t.Error("expected error for O2T size 2001 (> 2000)")
	}
}

func TestMapExplicit(t *testing.T) {
	rs, err := extractSample(t).mapExplicit(newNameSet())
	if err != nil {
		t.Fatalf("mapExplicit: %v", err)
	}
	// The sample has four explicit params (Param20-23), all read-only.
	if len(rs) != 4 {
		t.Fatalf("explicit resources: got %d, want 4", len(rs))
	}
	// SerialNumber (Param23, Link Path 20 01 24 01 30 06) -> objClass 1/inst 1/attr 6.
	sn := resourceByName(t, rs, "SerialNumber (explicit)")
	if sn.Attributes[attrObjClass] != 1 || sn.Attributes[attrInstID] != 1 || sn.Attributes[attrAttrID] != 6 {
		t.Errorf("SerialNumber attrs: got %v", sn.Attributes)
	}
	if _, ok := sn.Attributes[attrType]; ok {
		t.Error("explicit resource must not have a type attribute")
	}
	if sn.Properties.ReadWrite != common.ReadWrite_R {
		t.Errorf("explicit readWrite: got %s, want R", sn.Properties.ReadWrite)
	}
}

// End-to-end: the full sample converts to a profile that passes EdgeX validation
// and carries all three resource kinds (implicit I/O, settings, explicit).
func TestConvertSampleProducesAllKinds(t *testing.T) {
	p, err := extractSample(t).mapToProfile()
	if err != nil {
		t.Fatalf("mapToProfile: %v", err)
	}
	if verr := dtos.ValidateDeviceProfileDTO(p); verr != nil {
		t.Fatalf("validation: %v", verr)
	}
	var io, settings, explicit int
	for _, r := range p.DeviceResources {
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
	if io == 0 || settings != 3 || explicit != 4 {
		t.Errorf("resource kinds: io=%d settings=%d explicit=%d, want io>0 settings=3 explicit=4", io, settings, explicit)
	}
}

// M2: an explicit param with an unsupported data type errors with the param
// name in the message (parity with the implicit path's diagnostics).
func TestMapExplicitErrorNamesParam(t *testing.T) {
	x := &extracted{
		params: map[string]param{
			"Param9": {name: "Bad", dataType: "0xDC", linkPath: "20 01 24 01 30 06"}, // EPATH type, unsupported
		},
	}
	_, err := x.mapExplicit(newNameSet())
	if err == nil {
		t.Fatal("expected error for unsupported explicit data type")
	}
	if !strings.Contains(err.Error(), "Param9") {
		t.Errorf("error should name the param, got: %s", err.Error())
	}
}

// M4: assembly members that overrun the declared Size are rejected.
func TestMapImplicitSizeOverrunErrors(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{{bitLength: "32", paramRef: "P1"}}, // 4 bytes
		map[string]param{"P1": {name: "X", dataType: "0xC4"}}, 1)
	x.assemblies["A"] = assembly{name: "A", assemblyID: 100, size: "2", // declared 2 < used 4
		members: x.assemblies["A"].members}
	_, err := x.mapImplicitIO(newNameSet())
	if err == nil {
		t.Fatal("expected error when members overrun declared size")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("kind: got %v", errors.Kind(err))
	}
}
