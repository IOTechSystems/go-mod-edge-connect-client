// Copyright (C) 2026 IOTech Ltd

package tia

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// convertString converts inline SCL, failing the test if conversion errors.
func convertString(t *testing.T, scl string, options map[string]any) dtos.DeviceProfile {
	t.Helper()
	return convertBytes(t, []byte(scl), options)
}

// The fixture paths are literals, not variables: a path built from a parameter
// is flagged as a file-inclusion risk by the linter, and the sibling eds tests
// pass a literal for the same reason.
func convertSample(t *testing.T) dtos.DeviceProfile {
	t.Helper()
	data, err := os.ReadFile("testdata/sample.scl")
	if err != nil {
		t.Fatalf("read sample.scl: %v", err)
	}
	return convertBytes(t, data, nil)
}

// convertBytes converts SCL bytes, failing the test if conversion errors.
func convertBytes(t *testing.T, data []byte, options map[string]any) dtos.DeviceProfile {
	t.Helper()
	profile, err := Convert(context.Background(), logger.NewMockClient(), data, options)
	if err != nil {
		t.Fatalf("convert: %v", err)
	}
	return profile
}

// resourceByName indexes a profile's resources for focused assertions.
func resourceByName(t *testing.T, p dtos.DeviceProfile) map[string]dtos.DeviceResource {
	t.Helper()
	m := make(map[string]dtos.DeviceResource, len(p.DeviceResources))
	for _, r := range p.DeviceResources {
		m[r.Name] = r
	}
	return m
}

// wrap a minimal non-optimised data block around member declarations.
func dataBlock(members string) string {
	return "DATA_BLOCK \"test_db\"\n{ S7_Optimized_Access := 'FALSE' }\nVERSION : 0.1\n" +
		"   STRUCT\n" + members + "   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"
}

// The DB number and profile name come from options, not the source file: a TIA
// export carries the block name but not its number.
func TestConvertOptions(t *testing.T) {
	scl := dataBlock("      a : Int;\n")

	t.Run("dbNumber", func(t *testing.T) {
		profile := convertString(t, scl, map[string]any{OptionDBNumber: 7})
		if got := profile.DeviceResources[0].Attributes[attrDBNumber]; got != 7 {
			t.Errorf("DB_number: got %v, want 7", got)
		}
	})

	// Omitting it must default to 1, not 0, which would address the wrong block.
	t.Run("dbNumber defaults to 1", func(t *testing.T) {
		profile := convertString(t, scl, nil)
		if got := profile.DeviceResources[0].Attributes[attrDBNumber]; got != 1 {
			t.Errorf("DB_number: got %v, want 1", got)
		}
	})

	t.Run("profileName overrides the block name", func(t *testing.T) {
		profile := convertString(t, scl, map[string]any{OptionProfileName: "override"})
		if profile.Name != "override" {
			t.Errorf("profile name: got %q, want %q", profile.Name, "override")
		}
	})
}

// The flag TIA writes for an optimized block must be rejected, not worked around
// (see checkConvertible).
func TestConvertRejectsOptimizedAccess(t *testing.T) {
	scl := "DATA_BLOCK \"opt\"\n{ S7_Optimized_Access := 'TRUE' }\nVERSION : 0.1\n" +
		"   STRUCT\n      a : Int;\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"

	_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
	if err == nil {
		t.Fatal("expected an error for an optimized-access block")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A profile with no resources is indistinguishable from a successful conversion
// for a caller, so it is rejected rather than returned empty.
func TestConvertRejectsEmptyResult(t *testing.T) {
	_, err := Convert(context.Background(), logger.NewMockClient(),
		[]byte("DATA_BLOCK \"empty\"\nVERSION : 0.1\n   STRUCT\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"), nil)
	if err == nil {
		t.Fatal("expected an error when no resources are produced")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// Where the declarations begin and end. A section boundary the parser misreads
// either swallows the block's own members or adopts someone else's.
func TestConvertParsesSectionBoundaries(t *testing.T) {
	// The BEGIN section holds initial values, which S7 XRT profiles do not
	// support. It must be ignored wholesale — including dot-notation
	// assignments, which look superficially like declarations.
	// The STRUCT is deliberately left unclosed, so BEGIN is the only thing that can
	// end the declarations. With END_STRUCT present it would terminate first and
	// the BEGIN arm would never be reached.
	t.Run("BEGIN section is ignored", func(t *testing.T) {
		scl := "DATA_BLOCK \"init\"\n{ S7_Optimized_Access := 'FALSE' }\nVERSION : 0.1\n" +
			"   STRUCT\n      a : Int;\n" +
			"BEGIN\n   a := 42;\n   c.PV := 100;\n   s := 'text';\nEND_DATA_BLOCK\n"

		profile := convertString(t, scl, nil)
		if len(profile.DeviceResources) != 1 {
			var names []string
			for _, r := range profile.DeviceResources {
				names = append(names, r.Name)
			}
			t.Errorf("got %d resources %v, want only [a]", len(profile.DeviceResources), names)
		}
	})

	t.Run("inline comment becomes the description", func(t *testing.T) {
		profile := convertString(t, dataBlock("      a : Int;   // motor speed\n"), nil)
		if got := profile.DeviceResources[0].Description; got != "motor speed" {
			t.Errorf("description: got %q, want %q", got, "motor speed")
		}
	})

	// A second STRUCT in the block belongs to nothing: section resets after the
	// first, so its members must be discarded rather than replace the real ones.
	t.Run("a STRUCT outside any block is ignored", func(t *testing.T) {
		scl := "DATA_BLOCK \"d\"\n{ S7_Optimized_Access := 'FALSE' }\n" +
			"   STRUCT\n      real : Int;\n   END_STRUCT;\n" +
			"   STRUCT\n      ghost : Int;\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"
		byName := resourceByName(t, convertString(t, scl, nil))
		if _, ok := byName["ghost"]; ok {
			t.Error("a STRUCT outside the block's own must not emit resources")
		}
		if r, ok := byName["real"]; !ok || r.Attributes[attrStart] != 0 {
			t.Errorf("real: got start %v, want 0", r.Attributes[attrStart])
		}
	})
}

// Syntax within a single declaration line: an attribute block, a block comment
// or an invalid character must not cost the declaration its resource.
func TestConvertParsesDeclarationSyntax(t *testing.T) {

	// A (* *) comment is dropped, but it must neither merge the tokens it sat
	// between nor merge the lines it spanned.
	t.Run("block comments separate rather than weld", func(t *testing.T) {
		members := "      a : Int;   (* spanning\n      several\n      lines *)   b : Int;\n      c : Int;\n"
		byName := resourceByName(t, convertString(t, dataBlock(members), nil))
		for name, wantStart := range map[string]int{"a": 0, "b": 2, "c": 4} {
			r, ok := byName[name]
			if !ok {
				t.Errorf("%q missing: a multi-line comment merged its declaration line", name)
				continue
			}
			if got := r.Attributes[attrStart]; got != wantStart {
				t.Errorf("%q start: got %v, want %d", name, got, wantStart)
			}
		}
	})

	// Attribute blocks may appear on either side of the colon and carry no
	// profile meaning, so both must be stripped without disturbing the name or
	// the type.
	t.Run("attribute blocks are stripped", func(t *testing.T) {
		members := "      a { ExternalWritable := 'False'} : Int;\n      b : Int;\n"
		byName := resourceByName(t, convertString(t, dataBlock(members), nil))
		if _, ok := byName["a"]; !ok {
			t.Error("attribute block on the name side was not stripped")
		}
		if r, ok := byName["b"]; !ok || r.Properties.ValueType != common.ValueTypeInt16 {
			t.Error("declaration following an attribute block was not parsed")
		}
	})

	// A name with characters EdgeX rejects is sanitised: "q-name" -> q_name.
	t.Run("invalid characters in names are sanitised", func(t *testing.T) {
		byName := resourceByName(t, convertSample(t))
		if _, ok := byName["q_name"]; !ok {
			t.Error(`"q-name" should be emitted as q_name`)
		}
	})
}

// Measuring a UDT is cached per type. Without that, a type referenced from
// several places is re-walked once per reference path, so a chain of UDTs each
// holding N references to the next costs N^depth: ~1.3 KB of input took a second,
// and a few hundred more bytes would take hours.
func TestConvertUDTSizingIsNotExponential(t *testing.T) {
	// Each level holds 3 references to the next, so the naive walk is 3^depth.
	const fan, levels = 3, 40
	var b strings.Builder
	for l := 0; l < levels; l++ {
		fmt.Fprintf(&b, "TYPE \"T%d\"\n   STRUCT\n", l)
		if l == levels-1 {
			b.WriteString("      leaf : Int;\n")
		} else {
			for f := 0; f < fan; f++ {
				fmt.Fprintf(&b, "      c%d : \"T%d\";\n", f, l+1)
			}
		}
		b.WriteString("   END_STRUCT;\nEND_TYPE\n")
	}
	b.WriteString(dataBlock("      root : \"T0\";\n"))

	done := make(chan struct{})
	go func() {
		defer close(done)
		// UDT members are not expanded yet, so this yields no resources and errors
		// on the empty result; the point is that it finishes at all.
		_, _ = Convert(context.Background(), logger.NewMockClient(), []byte(b.String()), nil)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("UDT sizing did not finish: the per-type cache is not working")
	}
}

// A UDT definition (TYPE ... END_TYPE) sits alongside the data block in the same
// export. The parser must not mistake the UDT's STRUCT for the data block's own
// body — doing so emits a profile for the wrong block, silently, with
// plausible-looking offsets.
func TestConvertUDTFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/udt.scl")
	if err != nil {
		t.Fatalf("read udt.scl: %v", err)
	}
	profile := convertBytes(t, data, nil)

	// The file defines two UDTs before the data block, and both use STRUCT for
	// their declaration block. The profile must come from the DATA_BLOCK, not
	// from whichever STRUCT appears first.
	if profile.Name != "udt_db" {
		t.Errorf("profile name: got %q, want %q", profile.Name, "udt_db")
	}
	byName := resourceByName(t, profile)
	for _, fromUDT := range []string{"Speed", "Running", "Name", "station_id"} {
		if _, ok := byName[fromUDT]; ok {
			t.Errorf("%q comes from a UDT definition, not the data block", fromUDT)
		}
	}

	// UDT members are not emitted yet, but their size must still advance the
	// cursor. tail's offset is what proves both UDTs were measured correctly:
	// lead_in(2) + udt_Motor(26) + udt_Station(54) = 82.
	if len(profile.DeviceResources) != 2 {
		t.Fatalf("got %d resources, want 2 (lead_in and tail)", len(profile.DeviceResources))
	}
	if got := byName["lead_in"].Attributes[attrStart]; got != 0 {
		t.Errorf("lead_in start: got %v, want 0", got)
	}
	if got := byName["tail"].Attributes[attrStart]; got != 82 {
		t.Errorf("tail start: got %v, want 82", got)
	}

	// tail's offset alone would also hold if the two sizes were wrong by equal and
	// opposite amounts, so pin each one.
	src, perr := parseSCL(logger.NewMockClient(), string(data))
	if perr != nil {
		t.Fatalf("parse udt.scl: %v", perr)
	}
	f := &flattener{
		off:       &offsetTracker{},
		udts:      src.udts,
		names:     map[string]string{},
		resolving: map[string]bool{},
		sizes:     map[string]int{},
		resources: &[]dtos.DeviceResource{},
		lc:        logger.NewMockClient(),
		quiet:     true,
	}
	for udt, want := range map[string]int{"UDT_MOTOR": 26, "UDT_STATION": 54} {
		got, err := f.sizeOfUDT(udt, udt)
		if err != nil {
			t.Errorf("%s: %v", udt, err)
			continue
		}
		if got != want {
			t.Errorf("%s size: got %d, want %d", udt, got, want)
		}
	}
}

// A UDT may reference another UDT, and the export order does not guarantee a
// definition precedes its use — udt.scl defines udt_Station, which depends on
// udt_Motor, first. Sizes therefore cannot be resolved while scanning; every
// definition has to be collected before any is measured.
func TestConvertUDTDefinedBeforeItsDependency(t *testing.T) {
	scl := `TYPE "udt_Outer"
   STRUCT
      inner : "udt_Inner";
      tag : Int;
   END_STRUCT;
END_TYPE

TYPE "udt_Inner"
   STRUCT
      a : Int;
      b : Int;
   END_STRUCT;
END_TYPE

DATA_BLOCK "d"
{ S7_Optimized_Access := 'FALSE' }
   STRUCT
      o : "udt_Outer";
      after : Int;
   END_STRUCT;
BEGIN
END_DATA_BLOCK
`
	byName := resourceByName(t, convertString(t, scl, nil))

	// udt_Inner is 4 bytes, so udt_Outer is 4+2 = 6 and after follows it.
	if got := byName["after"].Attributes[attrStart]; got != 6 {
		t.Errorf("after start: got %v, want 6", got)
	}
}

// A UDT cycle cannot come from a clean TIA export, but a hand-edited file can
// contain one and the resolver is genuinely recursive.
func TestConvertRejectsUDTCycle(t *testing.T) {
	scl := `TYPE "udt_A"
   STRUCT
      to_b : "udt_B";
   END_STRUCT;
END_TYPE

TYPE "udt_B"
   STRUCT
      to_a : "udt_A";
   END_STRUCT;
END_TYPE

DATA_BLOCK "d"
{ S7_Optimized_Access := 'FALSE' }
   STRUCT
      a : "udt_A";
   END_STRUCT;
BEGIN
END_DATA_BLOCK
`
	_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
	if err == nil {
		t.Fatal("expected an error rather than unbounded recursion")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A UDT reference with no matching TYPE block has no known size, so no later
// offset can be derived.
func TestConvertRejectsUndefinedUDT(t *testing.T) {
	_, err := Convert(context.Background(), logger.NewMockClient(),
		[]byte(dataBlock("      m : \"udt_Missing\";\n")), nil)
	if err == nil {
		t.Fatal("expected an error for an unresolved PLC data type")
	}
	// The parser upper-cases type names, so match case-insensitively.
	if !strings.Contains(strings.ToUpper(err.Error()), "UDT_MISSING") {
		t.Errorf("error should name the missing type, got: %v", err)
	}
	if !strings.Contains(err.Error(), "dependent blocks") {
		t.Errorf("error should say how to fix it, got: %v", err)
	}
}

// An empty TYPE is stored as a nil member list, so a presence test that compares
// against nil reports it as undefined and tells the user to re-export with
// dependent blocks — for a type that is already in the file.
func TestConvertRecognisesEmptyUDT(t *testing.T) {
	scl := "TYPE \"udt_Empty\"\nVERSION : 0.1\n   STRUCT\n   END_STRUCT;\nEND_TYPE\n" +
		dataBlock("      lead : Int;\n      e : \"udt_Empty\";\n      tail : Int;\n")

	byName := resourceByName(t, convertString(t, scl, nil))

	// The UDT contributes no members and no size, so tail follows lead directly.
	if got := byName["tail"].Attributes[attrStart]; got != 2 {
		t.Errorf("tail start: got %v, want 2", got)
	}
}

// One profile describes one data block, and the DB number comes from options
// rather than the file, so a second block could not be numbered. Rejecting beats
// silently converting only the first.
func TestConvertRejectsMultipleDataBlocks(t *testing.T) {
	scl := "DATA_BLOCK \"first\"\n{ S7_Optimized_Access := 'FALSE' }\n   STRUCT\n      a : Int;\n" +
		"   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n" +
		"DATA_BLOCK \"second\"\n{ S7_Optimized_Access := 'FALSE' }\n   STRUCT\n      b : Int;\n" +
		"   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"

	_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
	if err == nil {
		t.Fatal("expected an error for a file with two data blocks")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A DB source file is untrusted input. Each of these once produced a plausible
// but wrong profile — negative offsets, a 254-byte allocation for String[0], or
// gigabytes of resources from a few hundred bytes of text.
func TestConvertRejectsHostileDeclarations(t *testing.T) {
	// Declarations that are skipped with a warning: the type is known to be
	// unusable but its size is not in doubt, so the cursor still advances and the
	// rest of the block is still correct.
	skipped := []struct {
		name    string
		members string
		wantB   int
	}{
		// A reversed range yields a negative count, which walked the cursor
		// backwards and emitted a negative array_size that Validate() accepts.
		{"reversed array range", "      a : Array[10..0] of Int;\n      b : Int;\n", 0},
		// An empty range is one element short of reversed: count 0 must be skipped
		// too, or it emits a resource whose array_size is 0.
		{"empty array range", "      a : Array[5..4] of Int;\n      b : Int;\n", 0},
		// Bool and String arrays emit one resource per element, so an unbounded
		// count is an amplification vector: this input is ~50 bytes.
		{"array element count over cap", "      a : Array[0..2000000000] of Bool;\n      b : Int;\n", 0},
		{"string longer than S7 allows", "      s : String[255];\n      b : Int;\n", 0},
		// The length is rejected while parsing, before the type is dispatched, so
		// this is a skip rather than an abort.
		{"string length overflows int", "      s : String[99999999999999999999];\n      b : Int;\n", 0},
	}
	for _, tt := range skipped {
		t.Run(tt.name, func(t *testing.T) {
			profile := convertString(t, dataBlock(tt.members), nil)
			byName := resourceByName(t, profile)
			if len(profile.DeviceResources) != 1 {
				t.Fatalf("got %d resources, want only b", len(profile.DeviceResources))
			}
			if got := byName["b"].Attributes[attrStart]; got != tt.wantB {
				t.Errorf("b start: got %v, want %d", got, tt.wantB)
			}
		})
	}

	// Declarations that abort the conversion: a bound that does not parse leaves
	// the type unrecognised, so its size is unknown and no later offset can be
	// derived. Atoi returns a clamped MaxInt64 alongside its error, which is why
	// the error must be checked rather than the value used.
	aborted := []struct {
		name    string
		members string
	}{
		// An unparsable bound leaves the whole ARRAY[...] signature unrecognised,
		// so it falls through to the unknown-type branch and aborts.
		{"array bound overflows int", "      a : Array[0..99999999999999999999] of Int;\n      b : Int;\n"},
	}
	for _, tt := range aborted {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(),
				[]byte(dataBlock(tt.members)), nil)
			if err == nil {
				t.Fatal("expected an error: the size is unknown, so later offsets cannot be derived")
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}
}

// String[0] is legal in TIA — a header with no capacity — and must not be
// confused with "no length declared", which takes the 254-byte default.
func TestConvertExplicitZeroLengthString(t *testing.T) {
	byName := resourceByName(t, convertString(t, dataBlock("      s : String[0];\n      b : Int;\n"), nil))

	if got := byName["s"].Attributes[attrSize]; got != 0 {
		t.Errorf("size: got %v, want 0", got)
	}
	// Header only: 2 bytes, so b follows immediately rather than at 256.
	if got := byName["b"].Attributes[attrStart]; got != 2 {
		t.Errorf("b start: got %v, want 2", got)
	}
}

// Array-of-String has its own length field, so the "no length declared" case has
// to be handled there too — an explicit String[0] inside an array must not pick
// up the 254-byte default.
func TestConvertArrayOfStringLengths(t *testing.T) {
	tests := []struct {
		name     string
		members  string
		wantSize int
		wantTail int
	}{
		// No brackets: each element takes the 254 default, so 2*(2+254) = 512.
		{"no length declared", "      a : Array[0..1] of String;\n      tail : Int;\n", 254, 512},
		{"explicit length", "      a : Array[0..1] of String[20];\n      tail : Int;\n", 20, 44},
		// Header only, 2 bytes per element.
		{"explicit zero length", "      a : Array[0..1] of String[0];\n      tail : Int;\n", 0, 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byName := resourceByName(t, convertString(t, dataBlock(tt.members), nil))
			a, ok := byName["a"]
			if !ok {
				t.Fatal("the array must be one resource, not one per element")
			}
			// size is the PER-ELEMENT length; array_size is the element count.
			if got := a.Attributes[attrSize]; got != tt.wantSize {
				t.Errorf("size: got %v, want %d", got, tt.wantSize)
			}
			if got := a.Attributes[attrArraySize]; got != 2 {
				t.Errorf("array_size: got %v, want 2", got)
			}
			if got := byName["tail"].Attributes[attrStart]; got != tt.wantTail {
				t.Errorf("tail start: got %v, want %d", got, tt.wantTail)
			}
		})
	}
}

// TIA quoting exists so a name can contain characters an identifier cannot.
// Both of these once failed the declaration regex and were dropped silently,
// taking the offset advance with them.
func TestConvertQuotedNamesWithSeparators(t *testing.T) {
	tests := []struct{ name, members, want string }{
		{"space in name", "      \"Motor Speed\" : Int;\n      after : Int;\n", "Motor_Speed"},
		{"colon in name", "      \"Tank:Level\" : Int;\n      after : Int;\n", "Tank_Level"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			byName := resourceByName(t, convertString(t, dataBlock(tt.members), nil))
			if _, ok := byName[tt.want]; !ok {
				var got []string
				for n := range byName {
					got = append(got, n)
				}
				t.Fatalf("resource %q not emitted, got %v", tt.want, got)
			}
			// The quoted variable occupies 2 bytes, so `after` follows it.
			if got := byName["after"].Attributes[attrStart]; got != 2 {
				t.Errorf("after start: got %v, want 2 (the quoted variable must still advance the cursor)", got)
			}
		})
	}
}

// The optimized-access flag must be honoured wherever it appears. Scanning only
// up to the first STRUCT made the one hard safety gate order-dependent.
func TestConvertRejectsOptimizedFlagAfterStruct(t *testing.T) {
	scl := "DATA_BLOCK \"opt\"\nVERSION : 0.1\n   STRUCT\n      a : Int;\n   END_STRUCT;\n" +
		"{ S7_Optimized_Access := 'TRUE' }\nBEGIN\nEND_DATA_BLOCK\n"

	_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
	if err == nil {
		t.Fatal("expected an error: the optimized flag must be found wherever it appears")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A wrong-typed or negative option is a caller mistake; silently ignoring it
// loses their intent with no signal, so it is rejected like any other bad input.
func TestConvertRejectsBadOptions(t *testing.T) {
	tests := []struct {
		name    string
		options map[string]any
	}{
		{"dbNumber not an int", map[string]any{OptionDBNumber: "5"}},
		{"dbNumber negative", map[string]any{OptionDBNumber: -7}},
		{"profileName not a string", map[string]any{OptionProfileName: 7}},
		{"misspelt key", map[string]any{"dbNumbr": 5}},
		{"another format's key", map[string]any{"assemblyID": 7}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(),
				[]byte(dataBlock("      a : Int;\n")), tt.options)
			if err == nil {
				t.Fatal("expected an error for an invalid option")
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}
}

// A caller-supplied name goes through the same sanitising as a derived one,
// otherwise the override can carry characters the derived path strips.
func TestConvertSanitizesOverriddenProfileName(t *testing.T) {
	profile := convertString(t, dataBlock("      a : Int;\n"),
		map[string]any{OptionProfileName: "bad name!"})

	if profile.Name != "bad_name_" {
		t.Errorf("profile name: got %q, want %q", profile.Name, "bad_name_")
	}
}

// Each skipped type must advance the cursor by its own size, not merely fail to
// emit. Asserting only the aggregate would let one size regress to 0 while
// another compensated.
func TestConvertEachSkipTypeAdvancesOffset(t *testing.T) {
	tests := []struct {
		s7 string
		// `a : Int` occupies 0-1, so the skipped type starts at 2 and `b` follows
		// it word-aligned. Only word alignment applies: an 8-byte type does not
		// force an 8-byte boundary.
		wantB int
	}{
		{"Char", 4}, // 1 byte at offset 2, so b aligns from 3 up to 4
		{"WChar", 4},
		{"DTL", 14},
		{"LTime", 10},
		{"DT", 10},
		{"LDT", 10}, // 8, NOT the design spec's 12
		{"HW_DEVICE", 4},
		{"CREF", 10},
		{"ErrorStruct", 30},
	}
	for _, tt := range tests {
		t.Run(tt.s7, func(t *testing.T) {
			members := "      a : Int;\n      k : " + tt.s7 + ";\n      b : Int;\n"
			byName := resourceByName(t, convertString(t, dataBlock(members), nil))
			if _, emitted := byName["k"]; emitted {
				t.Errorf("%s must not emit a resource", tt.s7)
			}
			if got := byName["b"].Attributes[attrStart]; got != tt.wantB {
				t.Errorf("b start: got %v, want %d", got, tt.wantB)
			}
		})
	}
}

// WString is skipped but occupies (2+n)*2 bytes — two bytes per character plus a
// two-word header. sample.scl has no WString, so without this a regression in
// allocWString is invisible.
func TestConvertWStringAdvancesOffset(t *testing.T) {
	for name, tc := range map[string]struct {
		decl  string
		wantB int
	}{
		// a occupies 0-1, then (2+5)*2 = 14 bytes, so b lands at 16.
		"scalar": {"      w : WString[5];\n", 16},
		// Three elements of (2+3)*2 = 10 bytes each, from 2: b lands at 32.
		"array": {"      w : Array[0..2] of WString[3];\n", 32},
		// No declared length, so each element takes the 254 default: 2*512 from 2.
		"array of bare WString": {"      w : Array[0..1] of WString;\n", 1026},
	} {
		t.Run(name, func(t *testing.T) {
			byName := resourceByName(t, convertString(t,
				dataBlock("      a : Int;\n"+tc.decl+"      b : Int;\n"), nil))

			if _, emitted := byName["w"]; emitted {
				t.Error("WString must not emit a resource: XRT would read two bytes per character")
			}
			if got := byName["b"].Attributes[attrStart]; got != tc.wantB {
				t.Errorf("b start: got %v, want %d", got, tc.wantB)
			}
		})
	}
}

// Every genuine TIA Portal export starts with a BOM, so without stripping it no
// real file converts at all (see Convert).
func TestConvertStripsUTF8BOM(t *testing.T) {
	const bom = "\ufeff"

	t.Run("a BOM does not hide the declarations", func(t *testing.T) {
		byName := resourceByName(t, convertString(t, bom+dataBlock("      a : Int;\n"), nil))
		if _, ok := byName["a"]; !ok {
			t.Errorf("got %d resources, want a", len(byName))
		}
	})

	t.Run("a BOM does not hide optimized access", func(t *testing.T) {
		scl := bom + "DATA_BLOCK \"opt\"\n{ S7_Optimized_Access := 'TRUE' }\nVERSION : 0.1\n" +
			"   STRUCT\n      a : Int;\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"
		_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
		if err == nil {
			t.Fatal("expected the optimized-access rejection")
		}
		if !strings.Contains(err.Error(), "optimized-access") {
			t.Errorf("error should name optimized access, got: %v", err)
		}
	})
}

// Each of these is legal SCL that the parser cannot measure, so it must abort
// rather than emit a plausible-looking profile (see parseVarBlock).
func TestConvertRejectsUnparsableDeclaration(t *testing.T) {
	for name, members := range map[string]string{
		"no name before the colon": "      a : Int;\n      : Int;\n      b : Int;\n",
		"comma-separated list":     "      a, b : Int;\n      c : Int;\n",
		"declaration split in two": "      a :\n         Int;\n      b : Int;\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(),
				[]byte(dataBlock(members)), nil)
			if err == nil {
				t.Fatal("expected an error: the skipped bytes would misalign everything after")
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}
}

// nestedBlock builds a data block whose members nest `depth` levels deep, with a
// single Int at the bottom and a sibling after the nest.
func nestedBlock(depth int) string {
	var b strings.Builder
	b.WriteString("DATA_BLOCK \"deep\"\n{ S7_Optimized_Access := 'FALSE' }\n   STRUCT\n")
	for i := 0; i < depth; i++ {
		fmt.Fprintf(&b, "      n%d : Struct\n", i)
	}
	b.WriteString("         x : Int;\n")
	for i := 0; i < depth; i++ {
		b.WriteString("      END_STRUCT;\n")
	}
	b.WriteString("      after : Int;\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n")
	return b.String()
}

// Deep STRUCT nesting was quadratic: walkMembers rebuilds the full dotted path at
// every level, so a few thousand levels — a small file — burned a second of CPU
// for one resource. Past the cap the body cannot be consumed, so the file is
// rejected rather than converted with the nest and everything after it missing.
func TestConvertBoundsStructNesting(t *testing.T) {
	// The deepest accepted nest still emits its innermost member.
	t.Run("at the cap", func(t *testing.T) {
		byName := resourceByName(t, convertString(t, nestedBlock(maxStructDepth-1), nil))
		want := fmt.Sprintf("n0_%s_x", strings.Join(nestedNames(1, maxStructDepth-1), "_"))
		if _, ok := byName[want]; !ok {
			t.Errorf("innermost member %q not emitted; got %d resources", want, len(byName))
		}
		if _, ok := byName["after"]; !ok {
			t.Error("the sibling after the nest must still be emitted")
		}
	})

	// Exactly at the cap is already too deep — the depth counter starts at 1 for
	// the block's own STRUCT. Using maxStructDepth rather than +1 is what pins the
	// comparison as >= : silently dropping the nest would also drop every
	// declaration after it, since its END_STRUCT lines unwind the parse.
	t.Run("past the cap", func(t *testing.T) {
		_, err := Convert(context.Background(), logger.NewMockClient(),
			[]byte(nestedBlock(maxStructDepth)), nil)
		if err == nil {
			t.Fatal("nesting past the cap must be rejected, not silently truncated")
		}
		if errors.Kind(err) != errors.KindContractInvalid {
			t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
		}
	})

	// A pathological depth must fail fast rather than hang.
	t.Run("does not hang", func(t *testing.T) {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = Convert(context.Background(), logger.NewMockClient(), []byte(nestedBlock(5000)), nil)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("conversion did not finish: struct nesting is not bounded")
		}
	})
}

// nestedNames lists n1..n(depth-1), the dotted path segments between the
// outermost struct and the innermost member.
func nestedNames(from, to int) []string {
	var names []string
	for i := from; i < to; i++ {
		names = append(names, fmt.Sprintf("n%d", i))
	}
	return names
}

// An over-long declaration line cannot be measured, so the bytes it occupies are
// unknown and every later address would be wrong. The eds parser rejects an
// over-long line for the same reason.
func TestConvertRejectsOverLongLine(t *testing.T) {
	scl := "DATA_BLOCK \"long\"\n{ S7_Optimized_Access := 'FALSE' }\n   STRUCT\n" +
		"      a : Int;\n      big : Int;" + strings.Repeat(" ", maxLineBytes+1) +
		"\n      c : Int;\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"

	_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
	if err == nil {
		t.Fatal("an over-long line must be rejected: skipping it misaligns every later address")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A type whose size is unknown must abort the conversion. Emitting the rest of
// the block would put every later resource at the wrong address — harder to
// notice than a failure, and XRT would read the wrong memory.
func TestConvertAbortsOnUnsizeableTypes(t *testing.T) {
	tests := []struct {
		name    string
		members string
		// namedInError identifies the offending declaration, so the user knows
		// which line to fix.
		namedInError string
	}{
		{"unknown scalar type", "      a : Int;\n      k : Nonesuch;\n      b : Int;\n", "NONESUCH"},
		{"array of struct", "      before : Int;\n      arr : Array[0..2] of Struct\n         x : Int;\n      END_STRUCT;\n      after : Int;\n", "arr"},
		{"unsupported array element", "      a : Array[0..2] of Nonesuch;\n", "NONESUCH"},
		// XRT's counterType attribute has no 64-bit value, so these are absent
		// from the counter table and fall through to the unknown-type branch.
		{"64-bit IEC counter", "      c : IEC_LCOUNTER;\n", "IEC_LCOUNTER"},
		{"64-bit unsigned IEC counter", "      c : IEC_ULCOUNTER;\n", "IEC_ULCOUNTER"},
		// An array element's String length is checked separately from a scalar's,
		// and both array bounds are converted, so both need the overflow check.
		// Failing parseArraySig leaves the whole type unrecognised.
		{"array element string over cap", "      a : Array[0..1] of String[255];\n", "a"},
		// Inside a STRUCT the error has to travel back out through walkMembers.
		// Swallowing it there leaves the members unmeasured but the profile valid.
		{"unknown type nested in a struct",
			"      s : Struct\n         bad : Nonesuch;\n      END_STRUCT;\n      tail : Int;\n", "s.bad"},
		{"array lower bound overflows int", "      a : Array[99999999999999999999..2] of Int;\n", "a"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(),
				[]byte(dataBlock(tt.members)), nil)
			if err == nil {
				t.Fatal("expected an error rather than a profile with drifted offsets")
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
			if !strings.Contains(err.Error(), tt.namedInError) {
				t.Errorf("error should name %q, got: %v", tt.namedInError, err)
			}
		})
	}
}

// Sanitising can map two distinct PLC variables onto one resource name. EdgeX
// would reject the duplicate, but only by the collided name — the error has to
// name both sources or the user cannot tell which declaration to rename.
func TestConvertReportsNameCollisionSources(t *testing.T) {
	tests := []struct {
		name    string
		members string
		want    []string
	}{
		{
			"struct member vs top-level",
			"      s : Struct\n         a : Int;\n      END_STRUCT;\n      s_a : Int;\n",
			[]string{"s.a", "s_a"},
		},
		{
			"quoted name vs identifier",
			"      \"a b\" : Int;\n      a_b : Int;\n",
			[]string{"a b", "a_b"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(),
				[]byte(dataBlock(tt.members)), nil)
			if err == nil {
				t.Fatal("expected an error for colliding resource names")
			}
			for _, want := range tt.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error should name %q, got: %v", want, err)
				}
			}
		})
	}
}

// Only a STRUCT inside a DATA_BLOCK becomes device resources. A STRUCT belonging
// to a TYPE definition, or one outside any block, must not leak in — that is how
// the profile ended up describing a UDT body instead of the data block.
func TestConvertOnlyParsesStructInsideDataBlock(t *testing.T) {
	decoy := `TYPE "udt_Decoy"
   STRUCT
      decoy_a : Int;
      decoy_b : Int;
   END_STRUCT;
END_TYPE
`
	block := `DATA_BLOCK "the_block"
{ S7_Optimized_Access := 'FALSE' }
VERSION : 0.1
NON_RETAIN
   STRUCT
      real_a : Int;
      real_b : Real;
   END_STRUCT;
BEGIN
END_DATA_BLOCK
`
	// Either order must give the same profile. A trailing TYPE is the sharper case:
	// its members are parsed after the block's, so leaking them would overwrite the
	// real ones rather than merely adding to them.
	for name, scl := range map[string]string{
		"TYPE before the block": decoy + "\n" + block,
		"TYPE after the block":  block + "\n" + decoy,
	} {
		t.Run(name, func(t *testing.T) {
			assertOnlyBlockMembers(t, convertString(t, scl, nil))
		})
	}
}

// assertOnlyBlockMembers checks that the profile holds the data block's two
// members at their own offsets, and nothing from the decoy TYPE.
func assertOnlyBlockMembers(t *testing.T, profile dtos.DeviceProfile) {
	t.Helper()
	byName := resourceByName(t, profile)

	if profile.Name != "the_block" {
		t.Errorf("profile name: got %q, want %q", profile.Name, "the_block")
	}
	if len(profile.DeviceResources) != 2 {
		var names []string
		for _, r := range profile.DeviceResources {
			names = append(names, r.Name)
		}
		t.Fatalf("got %d resources %v, want only the data block's two",
			len(profile.DeviceResources), names)
	}
	for _, d := range []string{"decoy_a", "decoy_b"} {
		if _, leaked := byName[d]; leaked {
			t.Errorf("%q belongs to a TYPE definition and must not be emitted", d)
		}
	}
	// The data block's own members keep their offsets, unaffected by the UDT.
	if got := byName["real_a"].Attributes[attrStart]; got != 0 {
		t.Errorf("real_a start: got %v, want 0", got)
	}
	if got := byName["real_b"].Attributes[attrStart]; got != 2 {
		t.Errorf("real_b start: got %v, want 2", got)
	}
}

// A data block with no STRUCT section yields no resources, which Convert rejects.
// The message only states what happened; it deliberately does not guess why.
// TIA Portal can derive a whole data block from a PLC data type, but Siemens
// publishes no grammar for how that appears in an export, so identifying the case
// would be speculation.
func TestConvertRejectsDataBlockWithoutStruct(t *testing.T) {
	scl := "DATA_BLOCK \"no_struct\"\n{ S7_Optimized_Access := 'FALSE' }\nVERSION : 0.1\n" +
		"\"udt_Something\"\nBEGIN\nEND_DATA_BLOCK\n"

	_, err := Convert(context.Background(), logger.NewMockClient(), []byte(scl), nil)
	if err == nil {
		t.Fatal("expected an error when the data block has no declaration section")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// The complete conversion of sample.scl, one row per resource: every attribute
// and property the converter emits, in declaration order. The per-concern tests
// above explain WHY each value is what it is; this one guards the whole shape at
// once, so a change that adds, drops or reorders a resource — or quietly alters
// one attribute — cannot slip through.
//
// The offsets are the same golden values recorded in the fixture's appendix.
// wantResource is one expected row of the golden profile. The any-typed fields
// are nil where the attribute does not apply to that resource's type.
type wantResource struct {
	name         string
	start        int
	bitIndex     any // nil unless Bool
	size         any // nil unless String
	arraySize    any // nil unless an array resource
	counterType  any // nil unless IEC_Counter
	resourceType string
	valueType    string
	maximum      any // nil unless the type's range is narrower than its integer
	hasDesc      bool
	// readOnly is set where the declaration marks the variable unwritable for
	// external clients (ExternalWritable := 'False').
	readOnly bool
}

// assertResource checks every attribute and property of one emitted resource.
func assertResource(t *testing.T, got dtos.DeviceResource, w wantResource) {
	t.Helper()
	if got.Name != w.name {
		t.Fatalf("name: got %q, want %q (resource order changed)", got.Name, w.name)
	}
	if got.Attributes[attrStart] != w.start {
		t.Errorf("start: got %v, want %d", got.Attributes[attrStart], w.start)
	}
	for _, f := range []struct {
		key  string
		want any
	}{
		{attrBitIndex, w.bitIndex},
		{attrSize, w.size},
		{attrArraySize, w.arraySize},
		{attrCounterType, w.counterType},
	} {
		if got.Attributes[f.key] != f.want {
			t.Errorf("%s: got %v, want %v", f.key, got.Attributes[f.key], f.want)
		}
	}
	if got.Attributes[attrType] != w.resourceType {
		t.Errorf("type: got %v, want %q", got.Attributes[attrType], w.resourceType)
	}
	if got.Attributes[attrDBNumber] != 1 {
		t.Errorf("DB_number: got %v, want 1", got.Attributes[attrDBNumber])
	}
	if got.Properties.ValueType != w.valueType {
		t.Errorf("valueType: got %q, want %q", got.Properties.ValueType, w.valueType)
	}
	assertMaximum(t, got, w.maximum)
	// An inline comment on the declaration becomes the description.
	if hasDesc := got.Description != ""; hasDesc != w.hasDesc {
		t.Errorf("description present: got %v, want %v (%q)", hasDesc, w.hasDesc, got.Description)
	}
	wantRW := common.ReadWrite_RW
	if w.readOnly {
		wantRW = common.ReadWrite_R
	}
	if got.Properties.ReadWrite != wantRW {
		t.Errorf("readWrite: got %q, want %q", got.Properties.ReadWrite, wantRW)
	}
}

// assertMaximum compares the emitted bound, which is a *float64, against the
// table's int-or-nil column.
func assertMaximum(t *testing.T, got dtos.DeviceResource, want any) {
	t.Helper()
	switch {
	case want == nil:
		if got.Properties.Maximum != nil {
			t.Errorf("maximum: got %v, want none", *got.Properties.Maximum)
		}
	case got.Properties.Maximum == nil:
		t.Errorf("maximum: got none, want %v", want)
	case *got.Properties.Maximum != float64(want.(int)):
		t.Errorf("maximum: got %v, want %v", *got.Properties.Maximum, want)
	}
}

// The cap must reject one element past it and accept the last legal count: the
// hostile-input test uses two billion, which cannot tell > from >=.
func TestConvertArrayElementCapBoundary(t *testing.T) {
	for name, tc := range map[string]struct {
		hi      int
		emitted bool
	}{
		"exactly at the cap": {maxArrayElements - 1, true},
		"one past the cap":   {maxArrayElements, false},
	} {
		t.Run(name, func(t *testing.T) {
			members := fmt.Sprintf("      a : Array[0..%d] of Int;\n      b : Int;\n", tc.hi)
			byName := resourceByName(t, convertString(t, dataBlock(members), nil))
			if _, ok := byName["a"]; ok != tc.emitted {
				t.Errorf("array emitted: got %v, want %v", ok, tc.emitted)
			}
			// b sits at 0 when the array was skipped, since the cursor never moved.
			if !tc.emitted {
				if got := byName["b"].Attributes[attrStart]; got != 0 {
					t.Errorf("b start: got %v, want 0", got)
				}
			}
		})
	}
}

// An array resource addresses the whole array, so a per-element bound would have
// XRT enforce a scalar range against it. TOD and LTOD are the only element types
// with a maximum, and neither appears as an array in the fixture.
func TestConvertArrayOfBoundedTypeHasNoMaximum(t *testing.T) {
	byName := resourceByName(t, convertString(t,
		dataBlock("      a : Array[0..2] of TOD;\n"), nil))

	a, ok := byName["a"]
	if !ok {
		t.Fatal("the array must be emitted")
	}
	if a.Properties.ValueType != common.ValueTypeUint32Array {
		t.Errorf("valueType: got %q, want %s", a.Properties.ValueType, common.ValueTypeUint32Array)
	}
	if a.Attributes[attrArraySize] != 3 {
		t.Errorf("array_size: got %v, want 3", a.Attributes[attrArraySize])
	}
	if a.Properties.Maximum != nil {
		t.Errorf("maximum: got %v, want none", *a.Properties.Maximum)
	}
}

// LTOD's bound is nanoseconds, three orders past TOD's, and it travels as a
// *float64 in the DTO. sample.scl has no LTOD, so nothing else checks that the
// value survives the trip to the profile intact.
func TestConvertEmitsLTODMaximum(t *testing.T) {
	byName := resourceByName(t, convertString(t,
		dataBlock("      lt : LTOD;\n      lt_alias : LTime_Of_Day;\n"), nil))

	for _, name := range []string{"lt", "lt_alias"} {
		r, ok := byName[name]
		if !ok {
			t.Fatalf("%q not emitted", name)
		}
		if r.Properties.ValueType != common.ValueTypeUint64 {
			t.Errorf("%s valueType: got %q, want %s", name, r.Properties.ValueType,
				common.ValueTypeUint64)
		}
		if r.Properties.Maximum == nil {
			t.Fatalf("%s: no maximum emitted", name)
		}
		if got := *r.Properties.Maximum; got != 86_399_999_999_999 {
			t.Errorf("%s maximum: got %v, want 86399999999999", name, got)
		}
	}
}

func TestConvertSampleProfile(t *testing.T) {
	want := []wantResource{
		{"s_default", 0, nil, 254, nil, nil, typeDB, common.ValueTypeString, nil, false, false},
		{"i8", 256, nil, nil, nil, nil, typeDB, common.ValueTypeInt8, nil, true, false},
		{"u8", 257, nil, nil, nil, nil, typeDB, common.ValueTypeUint8, nil, true, false},
		{"i16", 258, nil, nil, nil, nil, typeDB, common.ValueTypeInt16, nil, false, false},
		{"u16", 260, nil, nil, nil, nil, typeDB, common.ValueTypeUint16, nil, false, false},
		{"i32", 262, nil, nil, nil, nil, typeDB, common.ValueTypeInt32, nil, false, false},
		{"u32", 266, nil, nil, nil, nil, typeDB, common.ValueTypeUint32, nil, false, false},
		{"f32", 270, nil, nil, nil, nil, typeDB, common.ValueTypeFloat32, nil, false, false},
		{"f64", 274, nil, nil, nil, nil, typeDB, common.ValueTypeFloat64, nil, false, false},
		{"i64", 282, nil, nil, nil, nil, typeDB, common.ValueTypeInt64, nil, false, false},
		{"u64", 290, nil, nil, nil, nil, typeDB, common.ValueTypeUint64, nil, false, false},
		{"w", 298, nil, nil, nil, nil, typeDB, common.ValueTypeUint16, nil, false, false},
		{"dw", 300, nil, nil, nil, nil, typeDB, common.ValueTypeUint32, nil, false, false},
		{"lw", 304, nil, nil, nil, nil, typeDB, common.ValueTypeUint64, nil, false, false},
		{"t", 312, nil, nil, nil, nil, typeDB, common.ValueTypeInt32, nil, false, false},
		{"d", 316, nil, nil, nil, nil, typeDB, common.ValueTypeUint16, nil, false, false},
		{"tod", 318, nil, nil, nil, nil, typeDB, common.ValueTypeUint32, 86399999, false, false},
		{"tod_alias", 322, nil, nil, nil, nil, typeDB, common.ValueTypeUint32, 86399999, false, false},
		{"b1", 326, 0, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b2", 326, 1, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b3", 326, 2, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b4", 326, 3, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b5", 326, 4, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b6", 326, 5, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b7", 326, 6, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b8", 326, 7, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"b9", 327, 0, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"after_bools", 328, nil, nil, nil, nil, typeDB, common.ValueTypeInt16, nil, true, false},
		{"s10", 330, nil, 10, nil, nil, typeDB, common.ValueTypeString, nil, true, false},
		{"s9", 342, nil, 9, nil, nil, typeDB, common.ValueTypeString, nil, true, false},
		{"a_i8", 432, nil, nil, 3, nil, typeDB, common.ValueTypeInt8Array, nil, false, false},
		{"a_i16", 436, nil, nil, 3, nil, typeDB, common.ValueTypeInt16Array, nil, false, false},
		{"a_i32", 442, nil, nil, 3, nil, typeDB, common.ValueTypeInt32Array, nil, false, false},
		{"a_f32", 454, nil, nil, 3, nil, typeDB, common.ValueTypeFloat32Array, nil, false, false},
		{"a_f64", 466, nil, nil, 3, nil, typeDB, common.ValueTypeFloat64Array, nil, false, false},
		{"a_bool1", 490, nil, nil, 3, nil, typeDB, common.ValueTypeBoolArray, nil, false, false},
		{"a_bool2", 492, nil, nil, 9, nil, typeDB, common.ValueTypeBoolArray, nil, true, false},
		{"a_str", 494, nil, 20, 3, nil, typeDB, common.ValueTypeStringArray, nil, false, false},
		{"st_x", 560, nil, nil, nil, nil, typeDB, common.ValueTypeInt16, nil, false, false},
		{"st_y", 562, 0, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"st_s", 564, nil, 10, nil, nil, typeDB, common.ValueTypeString, nil, false, false},
		{"st_inner_a", 576, nil, nil, nil, nil, typeDB, common.ValueTypeInt32, nil, false, false},
		{"st_inner_b", 580, 0, nil, nil, nil, typeDB, common.ValueTypeBool, nil, false, false},
		{"tmr", 582, nil, nil, nil, nil, typeIECTimer, common.ValueTypeObject, nil, false, false},
		{"c_s", 598, nil, nil, nil, "int8", typeIECCounter, common.ValueTypeObject, nil, true, false},
		{"c_us", 602, nil, nil, nil, "uint8", typeIECCounter, common.ValueTypeObject, nil, true, false},
		{"c", 606, nil, nil, nil, "int16", typeIECCounter, common.ValueTypeObject, nil, true, false},
		{"c_u", 612, nil, nil, nil, "uint16", typeIECCounter, common.ValueTypeObject, nil, true, false},
		{"c_d", 618, nil, nil, nil, "int32", typeIECCounter, common.ValueTypeObject, nil, true, false},
		{"c_ud", 630, nil, nil, nil, "uint32", typeIECCounter, common.ValueTypeObject, nil, true, false},
		{"q_name", 642, nil, nil, nil, nil, typeDB, common.ValueTypeInt16, nil, false, true},
	}

	profile := convertSample(t)
	// The count catches a skip type that starts emitting, but name them too: a
	// positional table cannot otherwise state that these declarations are absent.
	byName := resourceByName(t, profile)
	for _, skipped := range []string{
		"k_char", "k_wchar", "k_dtl", "k_ltime", "k_dt", "k_ldt", "k_hwdev", "k_cref", "k_err",
	} {
		if _, emitted := byName[skipped]; emitted {
			t.Errorf("%q is a skip type and must not emit a resource", skipped)
		}
	}
	if len(profile.DeviceResources) != len(want) {
		t.Fatalf("resource count: got %d, want %d", len(profile.DeviceResources), len(want))
	}
	for i, w := range want {
		got := profile.DeviceResources[i]
		t.Run(w.name, func(t *testing.T) {
			assertResource(t, got, w)
		})
	}
}

// TIA marks a variable unwritable for external clients with
// ExternalWritable := 'False'. XRT reaches a DB over that same external-client
// route, so the flag is the resource's readWrite: offering a variable as
// writable when the engineer marked it read-only invites a write the PLC refuses.
func TestConvertDerivesReadWriteFromExternalWritable(t *testing.T) {
	tests := []struct {
		name string
		decl string
		want string
	}{
		{"no attribute block", "      v : Int;\n", common.ReadWrite_RW},
		{"ExternalWritable False", "      v { ExternalWritable := 'False' } : Int;\n", common.ReadWrite_R},
		{"ExternalWritable True", "      v { ExternalWritable := 'True' } : Int;\n", common.ReadWrite_RW},
		// The attribute name and value are matched case-insensitively, and
		// whitespace around := varies between exports.
		{"lower case", "      v { externalwritable := 'false' } : Int;\n", common.ReadWrite_R},
		{"extra spacing", "      v { ExternalWritable  :=  'False' } : Int;\n", common.ReadWrite_R},
		// A block may carry several attributes; only this one matters here.
		{"alongside other attributes",
			"      v { ExternalAccessible := 'False'; ExternalWritable := 'False'; S7_SetPoint := 'True' } : Int;\n",
			common.ReadWrite_R},
		// Every resource kind must honour it, not just plain scalars.
		{"array", "      v { ExternalWritable := 'False' } : Array[0..1] of Int;\n", common.ReadWrite_R},
		{"string", "      v { ExternalWritable := 'False' } : String[4];\n", common.ReadWrite_R},
		{"IEC counter", "      v { ExternalWritable := 'False' } : IEC_COUNTER;\n", common.ReadWrite_R},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile := convertString(t, dataBlock(tt.decl), nil)
			if got := profile.DeviceResources[0].Properties.ReadWrite; got != tt.want {
				t.Errorf("readWrite: got %q, want %q", got, tt.want)
			}
		})
	}
}

// A read-only struct member must not make its siblings read-only, and vice
// versa: the attribute belongs to the declaration it appears on.
func TestConvertReadWriteIsPerDeclaration(t *testing.T) {
	members := "      a : Int;\n" +
		"      b { ExternalWritable := 'False' } : Int;\n" +
		"      c : Int;\n"
	byName := resourceByName(t, convertString(t, dataBlock(members), nil))

	for name, want := range map[string]string{
		"a": common.ReadWrite_RW,
		"b": common.ReadWrite_R,
		"c": common.ReadWrite_RW,
	} {
		if got := byName[name].Properties.ReadWrite; got != want {
			t.Errorf("%s readWrite: got %q, want %q", name, got, want)
		}
	}
}

// A struct closes with word-align padding. The struct must start on an EVEN
// address and hold an odd number of bytes for that padding to be observable: from
// an odd start the member's own alignment already absorbs it.
func TestConvertStructClosesOnAWordBoundary(t *testing.T) {
	members := "      s : Struct\n         x : SInt;\n      END_STRUCT;\n      tail : SInt;\n"
	byName := resourceByName(t, convertString(t, dataBlock(members), nil))

	if got := byName["s_x"].Attributes[attrStart]; got != 0 {
		t.Errorf("s_x start: got %v, want 0", got)
	}
	if got := byName["tail"].Attributes[attrStart]; got != 2 {
		t.Errorf("tail start: got %v, want 2 (the struct pads to a word)", got)
	}
}

// A Bool array's byte span is ceil(count/8): the tail's offset is the only thing
// that reveals a miscounted span, since the array itself reports only its start.
func TestConvertBoolArrayByteSpan(t *testing.T) {
	for name, tc := range map[string]struct {
		decl     string
		wantTail int
	}{
		"eight bools fill one byte":    {"      ba : Array[0..7] of Bool;\n", 1},
		"nine bools spill to a second": {"      ba : Array[0..8] of Bool;\n", 2},
	} {
		t.Run(name, func(t *testing.T) {
			byName := resourceByName(t, convertString(t,
				dataBlock(tc.decl+"      tail : SInt;\n"), nil))
			if got := byName["tail"].Attributes[attrStart]; got != tc.wantTail {
				t.Errorf("tail start: got %v, want %d", got, tc.wantTail)
			}
		})
	}
}

// A STRUCT contributes a name prefix, not a resource, so a scalar may reuse the
// struct's own name without colliding with it.
func TestConvertStructAndScalarMayShareAName(t *testing.T) {
	members := "      s : Struct\n         x : Int;\n      END_STRUCT;\n      s : Int;\n"
	byName := resourceByName(t, convertString(t, dataBlock(members), nil))

	if _, ok := byName["s_x"]; !ok {
		t.Error("the struct member must be emitted as s_x")
	}
	if got, ok := byName["s"]; !ok {
		t.Error("a scalar may reuse the struct's name")
	} else if got.Attributes[attrStart] != 2 {
		t.Errorf("s start: got %v, want 2", got.Attributes[attrStart])
	}
}

// The mirror of the case below: a Bool array must not begin inside the byte a
// preceding scalar Bool is using, or its first element resolves to that scalar's
// PLC bit and writing either corrupts the other.
func TestConvertBoolArrayAfterScalarBool(t *testing.T) {
	members := "      b1 : Bool;\n      ba : Array[0..2] of Bool;\n      tail : SInt;\n"
	byName := resourceByName(t, convertString(t, dataBlock(members), nil))

	if got := byName["b1"].Attributes[attrStart]; got != 0 {
		t.Errorf("b1 start: got %v, want 0", got)
	}
	// The array closes b1's byte and word-aligns, so it cannot share byte 0.
	if got := byName["ba"].Attributes[attrStart]; got != 2 {
		t.Errorf("ba start: got %v, want 2 (element 0 must not alias b1's bit)", got)
	}
	if _, has := byName["ba"].Attributes[attrBitIndex]; has {
		t.Error("a bool array addresses a byte, so it must not carry a bitIndex")
	}
	// A 1-byte tail: an Int tail would re-absorb a one-byte drift and hide it.
	if got := byName["tail"].Attributes[attrStart]; got != 3 {
		t.Errorf("tail start: got %v, want 3", got)
	}
}

// Every construct must align from an ODD cursor, which is where the alignment
// step actually does work. A leading SInt puts the cursor at 1; the SInt tail is
// what makes the drift visible — an Int tail would re-absorb it.
func TestConvertAlignsFromOddCursor(t *testing.T) {
	tests := []struct {
		name     string
		decl     string
		wantTail int
	}{
		// 1 -> pad to 2, then the construct's own size.
		{"skip type", "      s : DTL;\n", 14},
		// A 1-byte skip type takes the odd address instead: no pad byte, so the
		// tail lands at 2, not 3. Char is the only skip type this applies to.
		{"1-byte skip type", "      s : Char;\n", 2},
		{"IEC timer", "      s : TON;\n", 18},
		{"IEC counter", "      s : IEC_COUNTER;\n", 8},
		{"WString", "      s : WString[3];\n", 12},
		// An array aligns as a whole even when its elements are 1 byte and could
		// each sit at an odd address: 3 SInts occupy 2..4, so the tail is at 5.
		{"array of 1-byte elements", "      s : Array[0..2] of SInt;\n", 5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			members := "      lead : SInt;\n" + tt.decl + "      tail : SInt;\n"
			byName := resourceByName(t, convertString(t, dataBlock(members), nil))
			if got := byName["lead"].Attributes[attrStart]; got != 0 {
				t.Fatalf("lead start: got %v, want 0", got)
			}
			if got := byName["tail"].Attributes[attrStart]; got != tt.wantTail {
				t.Errorf("tail start: got %v, want %d", got, tt.wantTail)
			}
		})
	}
}

// A Bool array owns its bytes outright. Without closing the byte after the
// elements, a following scalar Bool packed into the array's leftover bits and
// resolved to the same PLC bit as an array element — writing one corrupted the
// other.
func TestConvertScalarBoolAfterBoolArray(t *testing.T) {
	members := "      ba : Array[0..2] of Bool;\n      b1 : Bool;\n      b2 : Bool;\n      tail : Int;\n"
	byName := resourceByName(t, convertString(t, dataBlock(members), nil))

	if got := byName["ba"].Attributes[attrStart]; got != 0 {
		t.Errorf("ba start: got %v, want 0", got)
	}
	// byte 0 belongs to the array, so the scalars start a fresh byte.
	for _, tc := range []struct {
		name  string
		start int
		bit   int
	}{
		{"b1", 1, 0},
		{"b2", 1, 1},
	} {
		r := byName[tc.name]
		if got := r.Attributes[attrStart]; got != tc.start {
			t.Errorf("%s start: got %v, want %d", tc.name, got, tc.start)
		}
		if got := r.Attributes[attrBitIndex]; got != tc.bit {
			t.Errorf("%s bitIndex: got %v, want %d", tc.name, got, tc.bit)
		}
	}
	if got := byName["tail"].Attributes[attrStart]; got != 2 {
		t.Errorf("tail start: got %v, want 2", got)
	}
}

// Re-exporting with dependent blocks — which the missing-UDT error tells users to
// do — produces exactly this shape: a genuine TRUE followed by a UDT's own
// attribute block (see parseSCL).
func TestConvertOptimizedFlagIsScopedToTheDataBlock(t *testing.T) {
	udt := "TYPE \"u\"\n{ S7_Optimized_Access := 'FALSE' }\n   STRUCT\n      x : Int;\n" +
		"   END_STRUCT;\nEND_TYPE\n"
	optimizedDB := "DATA_BLOCK \"d\"\n{ S7_Optimized_Access := 'TRUE' }\n   STRUCT\n" +
		"      m : \"u\";\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n"

	for _, tt := range []struct {
		name string
		scl  string
	}{
		{"UDT before the data block", udt + optimizedDB},
		{"UDT after the data block", optimizedDB + udt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Convert(context.Background(), logger.NewMockClient(), []byte(tt.scl), nil)
			if err == nil {
				t.Fatal("expected an error: the data block is optimized, whatever the UDT says")
			}
			if errors.Kind(err) != errors.KindContractInvalid {
				t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
			}
		})
	}

	// A standard-access block still converts with UDTs on either side of it.
	profile := convertString(t, udt+"DATA_BLOCK \"d\"\n{ S7_Optimized_Access := 'FALSE' }\n"+
		"   STRUCT\n      m : \"u\";\n      a : Int;\n   END_STRUCT;\nBEGIN\nEND_DATA_BLOCK\n", nil)
	if len(profile.DeviceResources) != 1 {
		t.Errorf("got %d resources, want 1 (the UDT member is skipped)", len(profile.DeviceResources))
	}
}

// TIA quoting exists so a name can hold characters an identifier cannot, "//"
// among them. Cutting the line there dropped the declaration and left the cursor
// un-advanced, so the next variable took its address.
func TestConvertQuotedNameContainingSlashes(t *testing.T) {
	members := "      \"a//b\" : Int;   // the real comment\n      z : Int;\n"
	byName := resourceByName(t, convertString(t, dataBlock(members), nil))

	r, ok := byName["a__b"]
	if !ok {
		t.Fatalf(`"a//b" should be emitted as a__b, got %v`, byName)
	}
	if got := r.Attributes[attrStart]; got != 0 {
		t.Errorf("a__b start: got %v, want 0", got)
	}
	// The trailing comment is still a comment.
	if r.Description != "the real comment" {
		t.Errorf("description: got %q, want %q", r.Description, "the real comment")
	}
	if got := byName["z"].Attributes[attrStart]; got != 2 {
		t.Errorf("z start: got %v, want 2 (the quoted name must advance the cursor)", got)
	}
}

// A UDT closes with word-align padding like an inline struct. The fixture UDTs
// are both even-sized and bool-free, so they would size correctly even without
// that step; these two shapes are the ones that expose it.
func TestConvertUDTSizeIsWordAligned(t *testing.T) {
	for name, tc := range map[string]struct {
		body     string
		wantTail int
	}{
		// 1 byte of content, padded to 2.
		"one byte member": {"      x : SInt;\n", 2},
		// A bool-only UDT still occupies a whole word.
		"one bool member": {"      x : Bool;\n", 2},
		// From an odd cursor the reference itself must word-align first: a lead
		// SInt puts the cursor at 1, so the UDT starts at 2 and the tail at 4.
		"aligns from an odd cursor": {"      x : Int;\n", 4},
	} {
		t.Run(name, func(t *testing.T) {
			lead := ""
			if name == "aligns from an odd cursor" {
				lead = "      lead : SInt;\n"
			}
			scl := "TYPE \"udt_T\"\n   STRUCT\n" + tc.body + "   END_STRUCT;\nEND_TYPE\n" +
				dataBlock(lead+"      u : \"udt_T\";\n      tail : SInt;\n")
			byName := resourceByName(t, convertString(t, scl, nil))
			if got := byName["tail"].Attributes[attrStart]; got != tc.wantTail {
				t.Errorf("tail start: got %v, want %d", got, tc.wantTail)
			}
		})
	}
}
