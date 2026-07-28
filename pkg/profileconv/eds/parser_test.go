// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func TestSplitFields(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		// The critical case: empty slots must be preserved by position.
		{"empty slots preserved", "0,,,0xC1", []string{"0", "", "", "0xC1"}},
		{"single field", "42", []string{"42"}},
		{"trailing empty", "1,2,", []string{"1", "2", ""}},
		{"leading empty", ",1,2", []string{"", "1", "2"}},
		{"whitespace trimmed", " 1 , 2 ", []string{"1", "2"}},
		{"quotes stripped", `"DO0","DO1"`, []string{"DO0", "DO1"}},
		{"comma inside quotes kept", `"a, b", c`, []string{"a, b", "c"}},
		{"hex preserved verbatim", "0x0000,0xC1", []string{"0x0000", "0xC1"}},
		// Escaped quote (odd backslash run): the \" stays inside the field, so
		// the following comma is not a separator.
		{"escaped quote keeps comma inside", `"a \" b, c", d`, []string{`a \" b, c`, "d"}},
		// Escaped backslash then a real delimiter (even run): \\" closes the
		// string, so the next comma DOES separate.
		{"escaped backslash closes quote", `"a\\", b`, []string{`a\\`, "b"}},
		// Only ONE pair of delimiter quotes is stripped, so content ending in an
		// escaped quote keeps it (a greedy Trim would eat that quote too).
		{"content ending in escaped quote", `"a\""`, []string{`a\"`}},
		{"content is only an escaped quote", `"\""`, []string{`\"`}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := splitFields(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("field count: got %d %q, want %d %q", len(got), got, len(tt.want), tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("field %d: got %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestStripComment(t *testing.T) {
	tests := []struct{ in, want string }{
		{"VendCode = 1;  $ a comment", "VendCode = 1;  "},
		{`Name = "a$b";  $ dollar in quotes kept`, `Name = "a$b";  `},
		{"$ whole line comment", ""},
		{"no comment here", "no comment here"},
	}
	for _, tt := range tests {
		if got := stripComment(tt.in); got != tt.want {
			t.Errorf("stripComment(%q): got %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParseBasicSections(t *testing.T) {
	src := `
[Device]
    VendName = "Demo Corp";   $ manufacturer
    ProdName = "DEMO-DIO8";

[Params]
    Param1 = 0,,,0x0000,0xC1,1,"DO0";
`
	e, err := parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	dev := e.section(sectionDevice)
	if dev == nil {
		t.Fatal("missing [Device] section")
	}
	if got := dev.entry("VendName").field(0); got != "Demo Corp" {
		t.Errorf("VendName: got %q, want %q", got, "Demo Corp")
	}

	// Positional field access on the dense [Params] line: field 4 is the CIP
	// data type (0xC1), which only survives if empty slots 1 and 2 are kept.
	p1 := e.section(sectionParams).entry("Param1")
	if got := p1.field(4); got != "0xC1" {
		t.Errorf("Param1 field 4 (data type): got %q, want %q", got, "0xC1")
	}
	if got := p1.field(6); got != "DO0" {
		t.Errorf("Param1 field 6 (name): got %q, want %q", got, "DO0")
	}
}

// A statement spans multiple physical lines with no continuation character;
// only the ";" ends it. (EDS has no line-continuation marker.)
func TestParseMultiLineStatement(t *testing.T) {
	src := `
[Assembly]
    Assem100 = "Outputs",
        "20 04 24 64 30 03",
        1, Param1;
`
	e, err := parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	a := e.section(sectionAssembly).entry("Assem100")
	if a == nil {
		t.Fatal("missing Assem100")
	}
	// The three lines must join into one entry.
	if got, want := len(a.fields), 4; got != want {
		t.Fatalf("Assem100 fields: got %d %q, want %d", got, a.fields, want)
	}
	if a.field(0) != "Outputs" || a.field(3) != "Param1" {
		t.Errorf("Assem100 fields not joined correctly: %q", a.fields)
	}
}

// assertDeviceAFields parses one EDS source, then checks that entry "A" of the
// [Device] section split into exactly want.
func assertDeviceAFields(t *testing.T, src string, want []string) {
	t.Helper()
	e, err := parse(strings.NewReader(src))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	a := e.section(sectionDevice).entry("A")
	if a == nil {
		t.Fatal("entry A missing")
	}
	if len(a.fields) != len(want) {
		t.Fatalf("fields: got %d %q, want %d %q", len(a.fields), a.fields, len(want), want)
	}
	for i := range want {
		if a.field(i) != want[i] {
			t.Errorf("field %d: got %q, want %q", i, a.field(i), want[i])
		}
	}
}

// A '\"' inside a quoted field is an escaped quote, not a string boundary: it
// must not flip quote state, so a "," after it stays inside the field and the
// real closing quote still ends the field correctly.
func TestParseEscapedQuoteInString(t *testing.T) {
	tests := []struct {
		name   string
		fields string // the field list after "A = "
		want   []string
	}{
		// One escaped quote, then an in-quote comma.
		{"single escape", `"a \" b, c", d`, []string{`a \" b, c`, "d"}},
		// A pair of escaped quotes around text, plus an in-quote comma; the
		// paired \" must not open/close the string, so it stays one field.
		{"paired escapes", `"he said \"hi\", ok", 42`, []string{`he said \"hi\", ok`, "42"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertDeviceAFields(t, "[Device]\n    A = "+tt.fields+";\n", tt.want)
		})
	}
}

// Per the CIP grammar a quoted string may not span physical lines; an unbalanced
// quote on a line is a contract error.
func TestParseUnterminatedStringErrors(t *testing.T) {
	_, err := parse(strings.NewReader("[Device]\n    A = \"open;\n"))
	if err == nil {
		t.Fatal("expected error for unterminated quoted string")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

func TestParseEntryOutsideSectionErrors(t *testing.T) {
	_, err := parse(strings.NewReader("Orphan = 1;\n"))
	if err == nil {
		t.Fatal("expected error for entry outside any section")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// Regression: a ";" inside a quoted field must not terminate the statement.
// A whole-line Contains check would end the statement at the quoted ";" and
// drop the fields on the following lines.
func TestParseSemicolonInsideQuotesNotTerminator(t *testing.T) {
	src := `
[Device]
    A = "desc; more",
        42,
        99;
`
	assertDeviceAFields(t, src, []string{"desc; more", "42", "99"})
}

// Regression: a statement not terminated by ";" before EOF must be
// reported, not silently dropped.
func TestParseUnterminatedStatementAtEOFErrors(t *testing.T) {
	_, err := parse(strings.NewReader("[Device]\n    Name = \"x\"\n"))
	if err == nil {
		t.Fatal("expected error for unterminated statement at EOF")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// parseSample parses the coverage EDS from testdata, failing the test on error.
// ethernetip-sample.eds is the single golden fixture; it is hand-authored to
// fire every parser path, so a per-case reader literal is unnecessary.
func parseSample(t *testing.T) *eds {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "ethernetip-sample.eds"))
	if err != nil {
		t.Fatalf("read sample: %v", err)
	}
	e, edgexErr := parse(strings.NewReader(string(data)))
	if edgexErr != nil {
		t.Fatalf("parse: %v", edgexErr)
	}
	return e
}

// The parser must ingest the full sample without erroring and expose its
// sections.
func TestParseSampleSections(t *testing.T) {
	e := parseSample(t)
	if len(e.sections) == 0 {
		t.Fatal("no sections parsed")
	}
	for _, want := range []string{sectionDevice, sectionParams, sectionAssembly, sectionConnectionManager} {
		if e.section(want) == nil {
			t.Errorf("expected section %q not found", want)
		}
	}
}

// ethernetip-sample.eds exercises the syntax edge cases (coverage marker [K]):
// unknown keywords, non-core sections and multi-value comma lists must all parse
// cleanly and preserve the golden [Assembly] field layout.
func TestParseCoverageEdgeCases(t *testing.T) {
	e := parseSample(t)

	// Unknown keyword is kept, not dropped. Vendor_Custom_Flag sits in the
	// [Device Classification] section in the sample.
	if e.section(sectionDeviceClassification).entry("Vendor_Custom_Flag") == nil {
		t.Error("unknown keyword Vendor_Custom_Flag was dropped")
	}
	// Non-core section is kept.
	if e.section("Ethernet Link Class") == nil {
		t.Error("non-core section [Ethernet Link Class] was dropped")
	}
	// Golden [Assembly] layout: Assem100 field 0 is the name, and the dense
	// member list survives field splitting.
	assem := e.section(sectionAssembly).entry("Assem100")
	if assem == nil {
		t.Fatal("missing Assem100")
	}
	if got := assem.field(0); got != "Output Assembly" {
		t.Errorf("Assem100 field 0: got %q, want %q", got, "Output Assembly")
	}
}

// Param5's description quotes a comma; it must stay one field, not split. Param6
// spans several lines with no continuation char; its fields must join into one
// entry. Both are embedded in the sample so the single fixture covers these paths.
func TestParseSampleMultiLineAndQuotedComma(t *testing.T) {
	params := parseSample(t).section(sectionParams)

	// Quoted comma: Param5 field 8 is the description "signed, 8-bit output".
	// If the comma split the field, field 8 would be "signed" and the type/
	// name/range fields after it would all shift.
	p5 := params.entry("Param5")
	if got := p5.field(8); got != "signed, 8-bit output" {
		t.Errorf("Param5 description (quoted comma): got %q, want %q", got, "signed, 8-bit output")
	}
	if got := p5.field(4); got != "0xC2" {
		t.Errorf("Param5 field 4 (data type) shifted by quoted comma: got %q, want %q", got, "0xC2")
	}

	// Multi-line: Param6's lines join, so field 4 (the CIP type) is still 0xC3.
	p6 := params.entry("Param6")
	if got := p6.field(4); got != "0xC3" {
		t.Errorf("Param6 field 4 after multi-line join: got %q, want %q", got, "0xC3")
	}
}

// --- Edge cases and error paths ---

func TestParseEmptyInput(t *testing.T) {
	e, err := parse(strings.NewReader(""))
	if err != nil {
		t.Fatalf("empty input should parse cleanly: %v", err)
	}
	if len(e.sections) != 0 {
		t.Errorf("empty input: got %d sections, want 0", len(e.sections))
	}
}

func TestParseCommentsOnly(t *testing.T) {
	e, err := parse(strings.NewReader("$ a comment\n$ another\n"))
	if err != nil {
		t.Fatalf("comment-only input should parse cleanly: %v", err)
	}
	if len(e.sections) != 0 {
		t.Errorf("comment-only input: got %d sections, want 0", len(e.sections))
	}
}

func TestParseCRLFLineEndings(t *testing.T) {
	e, err := parse(strings.NewReader("[Device]\r\n    Name = \"x\";\r\n"))
	if err != nil {
		t.Fatalf("CRLF input: %v", err)
	}
	// The trailing "\r" must not leak into the section name or field value.
	if e.section(sectionDevice) == nil {
		t.Fatal("Device section not found (CRLF leaked into name?)")
	}
	if got := e.section(sectionDevice).entry("Name").field(0); got != "x" {
		t.Errorf("field with CRLF: got %q, want %q", got, "x")
	}
}

func TestParseMalformedHeaderMissingBracket(t *testing.T) {
	_, err := parse(strings.NewReader("[Device\n"))
	if err == nil {
		t.Fatal("expected error for header missing ']'")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

func TestParseEmptyKeywordErrors(t *testing.T) {
	_, err := parse(strings.NewReader("[Device]\n    = 1;\n"))
	if err == nil {
		t.Fatal("expected error for empty keyword")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// Content after the closing "]" must not be silently dropped.
func TestParseContentAfterHeaderErrors(t *testing.T) {
	_, err := parse(strings.NewReader("[Device] VendCode = 1;\n"))
	if err == nil {
		t.Fatal("expected error for content after ']'")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A new section header while the previous statement is unterminated must be
// reported, not swallowed into the pending statement.
func TestParseHeaderWhileUnterminatedErrors(t *testing.T) {
	_, err := parse(strings.NewReader("[A]\n    Name = 1\n[B]\n    Other = 2;\n"))
	if err == nil {
		t.Fatal("expected error for header during unterminated statement")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A physical line longer than the scanner cap is an input problem, reported
// as KindContractInvalid rather than a server fault.
func TestParseOverlongLineErrors(t *testing.T) {
	huge := "[Device]\n    Name = \"" + strings.Repeat("a", 2*1024*1024) + "\";\n"
	_, err := parse(strings.NewReader(huge))
	if err == nil {
		t.Fatal("expected error for overlong line")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// A statement built from many short unterminated lines (no ";") must hit the
// accumulated-statement cap, not grow the buffer without bound. Each line is
// well under the per-line cap, so only the statement cap can stop it.
func TestParseOverlongStatementErrors(t *testing.T) {
	var b strings.Builder
	b.WriteString("[Device]\n    Name =\n")
	for b.Len() < 2*maxStatementBytes {
		b.WriteString("aaaaaaaa\n") // short line, never a ";"
	}
	_, err := parse(strings.NewReader(b.String()))
	if err == nil {
		t.Fatal("expected error for overlong statement")
	}
	if errors.Kind(err) != errors.KindContractInvalid {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindContractInvalid)
	}
}

// errReader always fails, to exercise the genuine read-error path.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

func TestParseReadErrorIsServerError(t *testing.T) {
	_, err := parse(errReader{})
	if err == nil {
		t.Fatal("expected error from failing reader")
	}
	if errors.Kind(err) != errors.KindServerError {
		t.Errorf("error kind: got %v, want %v", errors.Kind(err), errors.KindServerError)
	}
}

// A token with no "=" inside a section is tolerated (skipped), not an error.
func TestParseBareTokenTolerated(t *testing.T) {
	e, err := parse(strings.NewReader("[Device]\n    JustAToken;\n    Name = \"x\";\n"))
	if err != nil {
		t.Fatalf("bare token should be tolerated: %v", err)
	}
	dev := e.section(sectionDevice)
	if dev.entry("Name") == nil {
		t.Error("Name entry lost after bare token")
	}
	if got := len(dev.entries); got != 1 {
		t.Errorf("entries: got %d, want 1 (bare token should not create an entry)", got)
	}
}

// parseEntry splits on the first "=" only; a value containing "=" is kept.
func TestParseMultipleEqualsSplitsOnFirst(t *testing.T) {
	e, err := parse(strings.NewReader("[Device]\n    A = a=b, c;\n"))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	a := e.section(sectionDevice).entry("A")
	if a == nil {
		t.Fatal("entry A missing")
	}
	want := []string{"a=b", "c"}
	if len(a.fields) != len(want) || a.field(0) != want[0] || a.field(1) != want[1] {
		t.Errorf("fields: got %q, want %q", a.fields, want)
	}
}

func TestEntryFieldOutOfRange(t *testing.T) {
	en := &entry{keyword: "A", fields: []string{"x"}}
	if got := en.field(-1); got != "" {
		t.Errorf("field(-1): got %q, want empty", got)
	}
	if got := en.field(5); got != "" {
		t.Errorf("field(5): got %q, want empty", got)
	}
	if got := en.field(0); got != "x" {
		t.Errorf("field(0): got %q, want %q", got, "x")
	}
}
