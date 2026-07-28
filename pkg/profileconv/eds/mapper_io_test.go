// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"strings"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// resourceByName finds a resource in a slice, or fails the test.
func resourceByName(t *testing.T, rs []dtos.DeviceResource, name string) dtos.DeviceResource {
	t.Helper()
	for _, r := range rs {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("resource %q not found", name)
	return dtos.DeviceResource{}
}

// ioWant is the expected shape of one implicit-I/O resource. bitLen 0 means the
// bitLength attribute should be absent (Bool omits it).
type ioWant struct {
	typ                  string
	offBytes, offBits    int
	bitLen               int
	valueType, readWrite string
}

// assertIOResource checks one resource against its expected shape.
func assertIOResource(t *testing.T, r dtos.DeviceResource, w ioWant) {
	t.Helper()
	if got := r.Attributes[attrType]; got != w.typ {
		t.Errorf("type: got %v, want %s", got, w.typ)
	}
	if got := r.Attributes[attrOffsetBytes]; got != w.offBytes {
		t.Errorf("offsetBytes: got %v, want %d", got, w.offBytes)
	}
	if got := r.Attributes[attrOffsetBits]; got != w.offBits {
		t.Errorf("offsetBits: got %v, want %d", got, w.offBits)
	}
	if w.bitLen == 0 {
		if got, ok := r.Attributes[attrBitLength]; ok {
			t.Errorf("bitLength: got %v, want absent", got)
		}
	} else if got := r.Attributes[attrBitLength]; got != w.bitLen {
		t.Errorf("bitLength: got %v, want %d", got, w.bitLen)
	}
	if r.Properties.ValueType != w.valueType {
		t.Errorf("valueType: got %s, want %s", r.Properties.ValueType, w.valueType)
	}
	if r.Properties.ReadWrite != w.readWrite {
		t.Errorf("readWrite: got %s, want %s", r.Properties.ReadWrite, w.readWrite)
	}
}

// Golden offsets from the sample's appendix. Assem100 (O2T, output/W) packs
// three BOOLs into byte0, a 5-bit pad, then widening types; Assem101 (T2O,
// input/R) is the read side.
func TestMapImplicitIO_OffsetsAndDirection(t *testing.T) {
	rs, err := extractSample(t).mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}

	cases := map[string]ioWant{
		// O2T output side (byte offsets from the appendix). Bool omits bitLength,
		// matching XRT profiles.
		"DO Channel 0":      {typeO2T, 0, 0, 0, common.ValueTypeBool, common.ReadWrite_W},
		"DO Channel 1":      {typeO2T, 0, 1, 0, common.ValueTypeBool, common.ReadWrite_W},
		"DO Channel 2":      {typeO2T, 0, 2, 0, common.ValueTypeBool, common.ReadWrite_W},
		"Output SINT":       {typeO2T, 1, 0, 8, common.ValueTypeInt8, common.ReadWrite_W},
		"Output INT scaled": {typeO2T, 2, 0, 16, common.ValueTypeInt16, common.ReadWrite_W},
		"Output DINT":       {typeO2T, 4, 0, 32, common.ValueTypeInt32, common.ReadWrite_W},
		"Output REAL":       {typeO2T, 8, 0, 32, common.ValueTypeFloat32, common.ReadWrite_W},
		"Output Mode":       {typeO2T, 12, 0, 16, common.ValueTypeUint16, common.ReadWrite_W},
		// T2O input side.
		"Input LINT":  {typeT2O, 0, 0, 64, common.ValueTypeInt64, common.ReadWrite_R},
		"Input USINT": {typeT2O, 24, 0, 8, common.ValueTypeUint8, common.ReadWrite_R},
		"Input WORD":  {typeT2O, 26, 0, 16, common.ValueTypeUint16, common.ReadWrite_R},
	}
	for name, w := range cases {
		t.Run(name, func(t *testing.T) {
			assertIOResource(t, resourceByName(t, rs, name), w)
		})
	}
}

// Setpoint (Param24) is referenced by both Assem100 (O2T) and Assem101 (T2O).
// Per the XRT model that is two distinct resources — an O2T (W) one and a T2O
// (R) one — not a single RW resource. The colliding name is direction-suffixed
// so both survive ValidateDeviceProfileDTO's uniqueness check.
func TestMapImplicitIO_SharedParamSplitsByDirection(t *testing.T) {
	rs, err := extractSample(t).mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	// O2T side keeps the plain name (emitted first), W, at byte14.
	o2t := resourceByName(t, rs, "Setpoint")
	if o2t.Attributes[attrType] != typeO2T || o2t.Properties.ReadWrite != common.ReadWrite_W {
		t.Errorf("Setpoint (O2T): got type=%v rw=%s, want O2T/W", o2t.Attributes[attrType], o2t.Properties.ReadWrite)
	}
	if o2t.Attributes[attrOffsetBytes] != 14 {
		t.Errorf("Setpoint (O2T) offsetBytes: got %v, want 14", o2t.Attributes[attrOffsetBytes])
	}
	// T2O side is suffixed, R, at byte32.
	t2o := resourceByName(t, rs, "Setpoint_T2O")
	if t2o.Attributes[attrType] != typeT2O || t2o.Properties.ReadWrite != common.ReadWrite_R {
		t.Errorf("Setpoint_T2O: got type=%v rw=%s, want T2O/R", t2o.Attributes[attrType], t2o.Properties.ReadWrite)
	}
	if t2o.Attributes[attrOffsetBytes] != 32 {
		t.Errorf("Setpoint_T2O offsetBytes: got %v, want 32", t2o.Attributes[attrOffsetBytes])
	}
}

// Pad members (RESERVED_*) occupy offset but must not produce a resource. (Their
// effect on offset is asserted in TestMapImplicitIO_OffsetsAndDirection, where
// Output SINT lands at byte1 only if the 5-bit pad filled byte0.)
func TestMapImplicitIO_PadProducesNoResource(t *testing.T) {
	rs, err := extractSample(t).mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	for _, r := range rs {
		if r.Name == "RESERVED_pad5" || r.Name == "RESERVED_pad8" {
			t.Errorf("pad member %q produced a resource", r.Name)
		}
	}
}

// ioFixture builds a minimal extracted with one assembly used by n connections
// in the given direction, so edge cases can be exercised without a sample file.
func ioFixture(dir string, members []assemblyMember, params map[string]param, conns int) *extracted {
	x := &extracted{
		params:     params,
		assemblies: map[string]assembly{"A": {name: "A", assemblyID: 100, members: members}},
	}
	for i := 0; i < conns; i++ {
		c := connection{}
		if dir == typeO2T {
			c.o2tFormat = "A"
		} else {
			c.t2oFormat = "A"
		}
		x.connections = append(x.connections, c)
	}
	return x
}

// Several connections sharing one assembly must expand it once, not once
// per connection (which would duplicate resources and mangle names).
func TestMapImplicitIO_SharedAssemblyExpandedOnce(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{{bitLength: "16", paramRef: "P1"}},
		map[string]param{"P1": {name: "X", dataType: "0xC3"}},
		3, // three connections all referencing assembly A as O2T
	)
	rs, err := x.mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	if len(rs) != 1 {
		t.Fatalf("shared assembly should yield 1 resource, got %d (%+v)", len(rs), rs)
	}
	if rs[0].Name != "X" {
		t.Errorf("name: got %q, want X (no spurious suffix)", rs[0].Name)
	}
}

// Names that would still collide after the direction suffix get a numeric
// suffix so every resource name stays unique (ValidateDeviceProfileDTO requires it).
func TestNameSetUnique(t *testing.T) {
	n := newNameSet()
	got := []string{
		n.unique("X", typeO2T), // X
		n.unique("X", typeT2O), // X_T2O
		n.unique("X", typeT2O), // X_T2O taken -> X_2
		n.unique("X", typeT2O), // -> X_3
	}
	want := []string{"X", "X_T2O", "X_2", "X_3"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("unique[%d]: got %q, want %q", i, got[i], want[i])
		}
	}
}

// M1: a non-positive bit length is rejected, not turned into a bad offset.
func TestMapImplicitIO_NonPositiveBitLengthErrors(t *testing.T) {
	for _, bits := range []string{"-8", "0"} {
		t.Run(bits, func(t *testing.T) {
			x := ioFixture(typeO2T,
				[]assemblyMember{{bitLength: bits, paramRef: "P1"}},
				map[string]param{"P1": {name: "X", dataType: "0xC3"}}, 1)
			_, err := x.mapImplicitIO(newNameSet())
			if err == nil {
				t.Fatalf("expected error for bit length %q", bits)
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("kind: got %v", errors.Kind(err))
			}
		})
	}
}

// M2: a member referencing an unknown param gives an error naming the assembly
// and the missing param, not an opaque "empty CIP data type code".
func TestMapImplicitIO_DanglingParamRefErrors(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{{bitLength: "16", paramRef: "Ghost"}},
		map[string]param{}, 1)
	_, err := x.mapImplicitIO(newNameSet())
	if err == nil {
		t.Fatal("expected error for dangling param ref")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("kind: got %v", errors.Kind(err))
	}
	if msg := err.Error(); !strings.Contains(msg, "Ghost") {
		t.Errorf("error should name the missing param Ghost, got: %s", msg)
	}
}

// Data Size fallback: a member with empty bit length takes the param's Data Size
// (bytes) as its length; missing Data Size is an error.
func TestMapImplicitIO_DataSizeFallback(t *testing.T) {
	// USINT with Data Size 1 byte -> 8 bits.
	x := ioFixture(typeT2O,
		[]assemblyMember{{bitLength: "", paramRef: "P1"}},
		map[string]param{"P1": {name: "In", dataType: "0xC6", dataSize: "1"}}, 1)
	rs, err := x.mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	if rs[0].Attributes[attrBitLength] != 8 {
		t.Errorf("bitLength from Data Size: got %v, want 8", rs[0].Attributes[attrBitLength])
	}

	// No bit length and no Data Size -> error.
	x = ioFixture(typeT2O,
		[]assemblyMember{{bitLength: "", paramRef: "P1"}},
		map[string]param{"P1": {name: "In", dataType: "0xC6"}}, 1)
	if _, err := x.mapImplicitIO(newNameSet()); err == nil {
		t.Error("expected error when member has neither bit length nor Data Size")
	}
}

// A5: Bool omits bitLength; a wider type keeps it.
func TestMapImplicitIO_BoolOmitsBitLength(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{
			{bitLength: "1", paramRef: "B"},
			{bitLength: "16", paramRef: "I"},
		},
		map[string]param{
			"B": {name: "Flag", dataType: "0xC1"}, // BOOL
			"I": {name: "Word", dataType: "0xC3"}, // INT
		}, 1)
	rs, err := x.mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	if _, ok := resourceByName(t, rs, "Flag").Attributes[attrBitLength]; ok {
		t.Error("Bool resource should omit bitLength")
	}
	if resourceByName(t, rs, "Word").Attributes[attrBitLength] != 16 {
		t.Error("non-Bool resource should keep bitLength")
	}
}

// A single member whose bit length exceeds maxBitLength is rejected by
// checkBits (distinct from the accumulated-offset guard exercised below).
func TestMapImplicitIO_MemberBitLengthOverMaxErrors(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{
			{bitLength: "16016", paramRef: "PAD"}, // 16016 > maxBitLength (16000)
		},
		map[string]param{
			"PAD": {name: "RESERVED_big"},
		}, 1)
	_, err := x.mapImplicitIO(newNameSet())
	if err == nil {
		t.Fatal("expected error for member bit length over max")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("kind: got %v", errors.Kind(err))
	}
}

// An assembly with no members yields no resources and no error.
func TestMapImplicitIO_EmptyAssembly(t *testing.T) {
	x := ioFixture(typeO2T, nil, map[string]param{}, 1)
	rs, err := x.mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	if len(rs) != 0 {
		t.Errorf("empty assembly: got %d resources, want 0", len(rs))
	}
}

// An assembly declared in [Assembly] but referenced by no connection produces
// nothing (no I/O resource, no Settings) and no error: only assemblies a
// connection uses have a known direction, and EDS commonly declares extra
// unused/alternate assemblies.
func TestMapImplicitIO_UnreferencedAssemblyIgnored(t *testing.T) {
	x := &extracted{
		params: map[string]param{"P": {name: "Used", dataType: "0xC2"}},
		assemblies: map[string]assembly{
			"Used":   {name: "Used", assemblyID: 100, members: []assemblyMember{{bitLength: "8", paramRef: "P"}}},
			"Unused": {name: "Unused", assemblyID: 200, members: []assemblyMember{{bitLength: "8", paramRef: "P"}}},
		},
		connections: []connection{{o2tFormat: "Used"}}, // Unused not referenced
	}

	io, err := x.mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	if len(io) != 1 || io[0].Name != "Used" {
		t.Errorf("I/O resources: got %+v, want only the referenced assembly's member", io)
	}

	set, err := x.mapSettings(newNameSet())
	if err != nil {
		t.Fatalf("mapSettings: %v", err)
	}
	// Only the referenced assembly yields a Settings resource; none carries the
	// unreferenced assembly's id (200).
	for _, r := range set {
		if r.Attributes[attrAssemblyID] == 200 {
			t.Errorf("unreferenced assembly 200 leaked into settings: %+v", r)
		}
	}
	if len(set) != 1 {
		t.Errorf("settings: got %d, want 1 (only the referenced O2T assembly)", len(set))
	}
}

// Scaling / min / max / default from [Params] map into properties;
// scale = Mult ÷ Div. A resource with no such fields carries none.
func TestMapImplicitIO_ScalingProperties(t *testing.T) {
	rs, err := extractSample(t).mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	scaled := resourceByName(t, rs, "Output INT scaled")
	if scaled.Properties.Scale == nil || *scaled.Properties.Scale != 0.1 {
		t.Errorf("scale: got %v, want 0.1", scaled.Properties.Scale)
	}
	if scaled.Properties.Minimum == nil || *scaled.Properties.Minimum != -3200 {
		t.Errorf("minimum: got %v, want -3200", scaled.Properties.Minimum)
	}
	if scaled.Properties.Maximum == nil || *scaled.Properties.Maximum != 3200 {
		t.Errorf("maximum: got %v, want 3200", scaled.Properties.Maximum)
	}
	// A plain BOOL output carries no scaling/min/max.
	do0 := resourceByName(t, rs, "DO Channel 0")
	if do0.Properties.Scale != nil || do0.Properties.Minimum != nil {
		t.Errorf("DO0 should have no scale/min, got scale=%v min=%v", do0.Properties.Scale, do0.Properties.Minimum)
	}
}

// A member bit length or Data Size beyond the max is rejected, not overflowed
// into a bogus small value.
func TestMapImplicitIO_OversizeBitLengthErrors(t *testing.T) {
	cases := map[string]*extracted{
		"huge bitLength": ioFixture(typeO2T,
			[]assemblyMember{{bitLength: "9223372036854775807", paramRef: "P1"}},
			map[string]param{"P1": {name: "X", dataType: "0xC3"}}, 1),
		"huge Data Size": ioFixture(typeO2T,
			[]assemblyMember{{bitLength: "", paramRef: "P1"}},
			map[string]param{"P1": {name: "X", dataType: "0xC3", dataSize: "2305843009213693953"}}, 1),
	}
	for name, x := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := x.mapImplicitIO(newNameSet()); err == nil {
				t.Fatal("expected error for oversize bit length / Data Size")
			}
		})
	}
}

// A member with no param reference is a pad: it advances the offset (so the
// next member lands past it) but yields no resource. This is the empty-ref pad
// path, distinct from a RESERVED-named param.
func TestMapImplicitIO_EmptyRefPad(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{
			{bitLength: "8", paramRef: ""},    // pad, no ref
			{bitLength: "16", paramRef: "P1"}, // must land at byte1
		},
		map[string]param{"P1": {name: "X", dataType: "0xC3"}}, 1)
	rs, err := x.mapImplicitIO(newNameSet())
	if err != nil {
		t.Fatalf("mapImplicitIO: %v", err)
	}
	if len(rs) != 1 {
		t.Fatalf("empty-ref pad should yield 1 resource, got %d", len(rs))
	}
	if rs[0].Name != "X" || rs[0].Attributes[attrOffsetBytes] != 1 {
		t.Errorf("member after pad: got %q byte=%v, want X byte1", rs[0].Name, rs[0].Attributes[attrOffsetBytes])
	}
}

// The running offset is bounded: members accumulating past maxOffsetBytes are
// rejected (the per-member bit length is individually within limits).
func TestMapImplicitIO_OffsetOverMaxErrors(t *testing.T) {
	x := ioFixture(typeO2T,
		[]assemblyMember{
			{bitLength: "16000", paramRef: "P1"}, // 2000 bytes
			{bitLength: "16", paramRef: "P2"},    // reaches byte 2002
			{bitLength: "16", paramRef: "P3"},    // starts at offsetBytes 2002 > 2000
		},
		map[string]param{
			"P1": {name: "A", dataType: "0xC3"},
			"P2": {name: "B", dataType: "0xC3"},
			"P3": {name: "C", dataType: "0xC3"},
		}, 1)
	_, err := x.mapImplicitIO(newNameSet())
	if err == nil {
		t.Fatal("expected error when accumulated offset exceeds max")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("kind: got %v", errors.Kind(err))
	}
}
