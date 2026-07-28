// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func TestSanitizeName(t *testing.T) {
	tests := []struct{ in, want string }{
		{"EtherNetIP Sample", "ethernetip-sample"},
		{"DEMO-DIO8", "demo-dio8"}, // already an identifier
		{"AB/1756 Module", "ab-1756-module"},
		{"  spaced  ", "spaced"},       // leading/trailing separators trimmed
		{"a//b  c", "a-b-c"},           // runs of separators collapse to one
		{"Back\\slash", "back-slash"},  // backslash is a separator
		{"a - b", "a-b"},               // space+hyphen+space collapses, not "a---b"
		{"a---b", "a-b"},               // existing hyphen run collapses too
		{"/lead/trail/", "lead-trail"}, // leading/trailing slashes trimmed
		{"\ttab\t", "tab"},             // tab is a separator
		{"   ", ""},                    // all-whitespace sanitizes to empty
		{"///", ""},                    // all-separator sanitizes to empty
		{"", ""},                       // empty stays empty (caller handles)
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := sanitizeName(tt.in); got != tt.want {
				t.Errorf("sanitizeName(%q): got %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestMapToProfileIdentity(t *testing.T) {
	x := extractSample(t)
	p, err := x.mapToProfile()
	if err != nil {
		t.Fatalf("mapToProfile: %v", err)
	}
	if p.Name != "ethernetip-sample" {
		t.Errorf("Name: got %q, want ethernetip-sample", p.Name)
	}
	if p.Manufacturer != "Coverage Test Vendor" {
		t.Errorf("Manufacturer: got %q", p.Manufacturer)
	}
	if p.Model != "EtherNetIP Sample" {
		t.Errorf("Model: got %q, want EtherNetIP Sample (original, not sanitized)", p.Model)
	}
}

// model/name fall back to Catalog when ProdName is absent OR sanitizes to empty
// (e.g. whitespace-only) — the fallback keys off the sanitized result, not the
// raw string, so a whitespace ProdName does not shadow a usable Catalog.
func TestMapToProfileCatalogFallback(t *testing.T) {
	tests := []struct {
		name        string
		productName string
	}{
		{"prodname absent", ""},
		{"prodname whitespace", "   "}, // must still fall back to Catalog
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := (&extracted{device: deviceInfo{productName: tt.productName, catalog: "Fallback Cat"}}).mapToProfile()
			if err != nil {
				t.Fatalf("mapToProfile: %v", err)
			}
			if p.Model != "Fallback Cat" || p.Name != "fallback-cat" {
				t.Errorf("catalog fallback: got model=%q name=%q", p.Model, p.Name)
			}
			if p.Manufacturer != "" {
				t.Errorf("Manufacturer should be empty, got %q", p.Manufacturer)
			}
		})
	}
}

// No ProdName and no Catalog means no derivable name — an error, not an empty name.
func TestMapToProfileNoNameErrors(t *testing.T) {
	_, err := (&extracted{}).mapToProfile()
	if err == nil {
		t.Fatal("expected error when device has no ProdName/Catalog")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// An enumerated I/O param becomes a DeviceCommand whose single resourceOperation
// carries the value->label mappings and targets the emitted resource. A param
// without an enum produces no command.
func TestMapToProfileEnumCommand(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{
			{bitLength: "16", paramRef: "Mode"},
			{bitLength: "8", paramRef: "Plain"},
		},
		map[string]param{
			"Mode":  {name: "Mode", dataType: "0xC7", enum: map[string]string{"0": "Off", "1": "On"}},
			"Plain": {name: "Plain", dataType: "0xC2"},
		}, 1)
	x.device = deviceInfo{productName: "Dev"}

	p, err := x.mapToProfile()
	if err != nil {
		t.Fatalf("mapToProfile: %v", err)
	}
	if len(p.DeviceCommands) != 1 {
		t.Fatalf("device commands: got %d, want 1 (only the enum param)", len(p.DeviceCommands))
	}
	cmd := p.DeviceCommands[0]
	if cmd.Name != "Mode" || cmd.ReadWrite != common.ReadWrite_W {
		t.Errorf("command: got name=%q rw=%q, want Mode/W", cmd.Name, cmd.ReadWrite)
	}
	if len(cmd.ResourceOperations) != 1 || cmd.ResourceOperations[0].DeviceResource != "Mode" {
		t.Fatalf("resourceOperations: got %+v", cmd.ResourceOperations)
	}
	m := cmd.ResourceOperations[0].Mappings
	if m["0"] != "Off" || m["1"] != "On" || len(m) != 2 {
		t.Errorf("mappings: got %v, want {0:Off, 1:On}", m)
	}
}

// A bidirectional enum param (in both an O2T and a T2O assembly) becomes TWO
// resources (X and X_T2O) and TWO commands, each targeting its emitted
// (direction-suffixed) resource name. This locks the invariant that the command
// tracks the FINAL resource name, not the raw param name — using the param name
// would emit two commands both named X, which profile.Validate() rejects as a
// duplicate/dangling command.
func TestMapToProfileBidirectionalEnumCommands(t *testing.T) {
	x := &extracted{
		params: map[string]param{
			"Mode": {name: "Mode", dataType: "0xC7", enum: map[string]string{"0": "Off", "1": "On"}},
		},
		assemblies: map[string]assembly{
			"Out": {name: "Out", assemblyID: 100, members: []assemblyMember{{bitLength: "16", paramRef: "Mode"}}},
			"In":  {name: "In", assemblyID: 101, members: []assemblyMember{{bitLength: "16", paramRef: "Mode"}}},
		},
		connections: []connection{{o2tFormat: "Out", t2oFormat: "In"}},
		device:      deviceInfo{productName: "Dev"},
	}
	p, err := x.mapToProfile()
	if err != nil {
		t.Fatalf("mapToProfile: %v", err)
	}
	// Must pass EdgeX validation: two distinctly-named commands, each referencing
	// a resource that exists.
	if verr := p.Validate(); verr != nil {
		t.Fatalf("validation: %v", verr)
	}
	if len(p.DeviceCommands) != 2 {
		t.Fatalf("device commands: got %d, want 2 (O2T + T2O)", len(p.DeviceCommands))
	}
	// Command name -> (readWrite, target resource); both must be self-consistent
	// and distinct.
	byName := map[string]dtos.DeviceCommand{}
	for _, c := range p.DeviceCommands {
		byName[c.Name] = c
	}
	for name, wantRW := range map[string]string{"Mode": common.ReadWrite_W, "Mode_T2O": common.ReadWrite_R} {
		c, ok := byName[name]
		if !ok {
			t.Fatalf("missing command %q; got %v", name, byName)
		}
		if c.ReadWrite != wantRW {
			t.Errorf("%s readWrite: got %q, want %q", name, c.ReadWrite, wantRW)
		}
		if c.ResourceOperations[0].DeviceResource != name {
			t.Errorf("%s targets %q, want itself", name, c.ResourceOperations[0].DeviceResource)
		}
	}
}

// Mapping the same *extracted twice must be idempotent: enum scratch is per-pass,
// so the second profile has the same commands and still validates (no duplicate).
func TestMapToProfileIdempotent(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{{bitLength: "16", paramRef: "Mode"}},
		map[string]param{"Mode": {name: "Mode", dataType: "0xC7", enum: map[string]string{"0": "Off"}}}, 1)
	x.device = deviceInfo{productName: "Dev"}

	first, err := x.mapToProfile()
	if err != nil {
		t.Fatalf("first mapToProfile: %v", err)
	}
	second, err := x.mapToProfile()
	if err != nil {
		t.Fatalf("second mapToProfile: %v", err)
	}
	if len(first.DeviceCommands) != 1 || len(second.DeviceCommands) != 1 {
		t.Fatalf("commands: first=%d second=%d, want 1 each", len(first.DeviceCommands), len(second.DeviceCommands))
	}
	if verr := second.Validate(); verr != nil {
		t.Errorf("second profile failed validation (duplicate command?): %v", verr)
	}
}

// A profile with no enum params omits deviceCommands (nil), not an empty slice.
func TestMapToProfileNoEnumNoCommands(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{{bitLength: "8", paramRef: "P"}},
		map[string]param{"P": {name: "P", dataType: "0xC2"}}, 1)
	x.device = deviceInfo{productName: "Dev"}
	p, err := x.mapToProfile()
	if err != nil {
		t.Fatalf("mapToProfile: %v", err)
	}
	if p.DeviceCommands != nil {
		t.Errorf("deviceCommands: got %v, want nil", p.DeviceCommands)
	}
}
