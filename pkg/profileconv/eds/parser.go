// Copyright (C) 2026 IOTech Ltd

// This file is the EDS syntax layer: parse() reads EDS text into a generic,
// semantics-free tree of sections/entries/fields. Fields are split on
// out-of-quote commas with empty slots kept by position; each field is trimmed
// of surrounding whitespace and its delimiting quotes, but its inner content
// (including in-quote commas and escapes) is left verbatim.

package eds

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// EDS section names referenced across conversion stages. Defined once here so
// the parser, extractor and mapper agree on the exact spelling (names are
// matched case-sensitively as written in the EDS).
const (
	sectionDevice               = "Device"
	sectionDeviceClassification = "Device Classification"
	sectionParams               = "Params"
	sectionAssembly             = "Assembly"
	sectionConnectionManager    = "Connection Manager"
	sectionFile                 = "File"
	sectionModular              = "Modular"
)

// Size limits guarding against pathological input. A single physical line and a
// single ";"-terminated statement (which may span many lines) are each capped;
// the statement cap stops a file of short unterminated lines from being
// concatenated without bound. maxErrMsgLen keeps a malformed fragment from
// bloating an error message.
const (
	maxLineBytes      = 1024 * 1024
	maxStatementBytes = 1024 * 1024
	maxErrMsgLen      = 256
)

// eds is the parsed representation of one EDS file: an ordered list of sections.
// Order is preserved so callers relying on declaration order (e.g. assembly
// member layout) read fields as authored.
type eds struct {
	sections []*section
}

// section is a "[Name]" block containing zero or more entries.
type section struct {
	name    string
	entries []*entry
}

// entry is a single "Keyword = field0, field1, ...;" statement.
//
// fields holds every comma-separated field, each trimmed of surrounding
// whitespace and delimiting quotes, with empty slots kept as empty strings —
// e.g. "0,,,0xC1" parses to ["0", "", "", "0xC1"] and "\"DO0\", 1" to
// ["DO0", "1"]. Preserving empty slots is essential: EDS field meaning is
// positional, so a dropped empty slot would shift every later field.
type entry struct {
	keyword string
	fields  []string
}

// section returns the first section with the given name, or nil if absent.
// Section names are matched case-sensitively as written in the EDS.
func (e *eds) section(name string) *section {
	for _, s := range e.sections {
		if s.name == name {
			return s
		}
	}
	return nil
}

// sectionEntries returns the entries of every section with the given name,
// concatenated in file order. An EDS occasionally repeats a section (e.g. two
// [Params] blocks); reading only the first would silently drop the rest.
func (e *eds) sectionEntries(name string) []*entry {
	var entries []*entry
	for _, s := range e.sections {
		if s.name == name {
			entries = append(entries, s.entries...)
		}
	}
	return entries
}

// entry returns the first entry with the given keyword in the section, or nil.
func (s *section) entry(keyword string) *entry {
	for _, en := range s.entries {
		if en.keyword == keyword {
			return en
		}
	}
	return nil
}

// field returns field i (0-indexed), already trimmed at parse time, or "" if i
// is out of range. Out-of-range returns "" rather than panicking so callers can
// read optional trailing fields without bounds checks.
func (en *entry) field(i int) string {
	if i < 0 || i >= len(en.fields) {
		return ""
	}
	return en.fields[i]
}

// parse reads EDS text and returns its section/entry/field AST.
//
// It is purely syntactic — it understands the EDS grammar (sections, entries
// terminated by ";", comma-separated fields, double-quoted strings with "\"
// escapes, "$" comments) but attaches no CIP meaning. A statement spans multiple
// lines simply by omitting ";" until it ends; there is no line-continuation
// character. Unknown keywords and non-core sections are kept, not rejected, so
// later stages decide what to use.
//
// A genuine read error on r is errors.KindServerError; malformed content — an
// entry outside any section, a bad section header, an unterminated statement, or
// a line exceeding the length cap — is errors.KindContractInvalid.
func parse(r io.Reader) (*eds, errors.EdgeX) {
	e := &eds{}
	var cur *section        // section currently being filled; nil until first "[...]"
	var buf strings.Builder // accumulates a statement across its physical lines

	sc := bufio.NewScanner(r)
	// EDS entries (e.g. large assemblies) can exceed bufio's default 64KB line
	// cap once joined; raise the token limit to be safe.
	sc.Buffer(make([]byte, 0, 64*1024), maxLineBytes)

	lineNo := 0
	for sc.Scan() {
		lineNo++
		if err := consumeLine(e, &cur, &buf, sc.Text(), lineNo); err != nil {
			return nil, err
		}
	}
	if err := sc.Err(); err != nil {
		// A line exceeding the token cap is an input-size problem, not a server
		// fault; classify it as a contract violation.
		if err == bufio.ErrTooLong {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "EDS line exceeds the maximum supported length", err)
		}
		return nil, errors.NewCommonEdgeX(errors.KindServerError, "failed to read EDS input", err)
	}

	// A statement left in the buffer at EOF was never terminated by ";".
	// Rather than silently dropping it (which loses the file's last entry when
	// the trailing ";" is missing or the file is truncated), report it.
	if stmt := strings.TrimSpace(buf.String()); stmt != "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("unterminated statement at end of input (missing ';'): %s", truncateForErr(stmt)), nil)
	}
	return e, nil
}

// consumeLine processes one physical line: it rejects an unbalanced quote,
// dispatches a section header, skips a blank line, or accumulates the line into
// the pending statement — splitting out any statements it completes once the
// line contains an unquoted ";".
func consumeLine(e *eds, cur **section, buf *strings.Builder, raw string, lineNo int) errors.EdgeX {
	line := stripComment(raw) // "$" comment runs to end of line

	// A quoted string may not span lines (see scanLine), so an unbalanced quote
	// is an error.
	hasSemicolon, unbalanced := scanLine(line)
	if unbalanced {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("unterminated quoted string on line %d: %s", lineNo, strings.TrimSpace(line)), nil)
	}

	// Section headers stand alone on their own line, outside any pending statement.
	trimmed := strings.TrimSpace(line)
	handled, err := handleSectionHeader(e, cur, buf, trimmed)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}
	if buf.Len() == 0 && trimmed == "" {
		return nil // blank line between statements
	}

	// Accumulate into the current statement. A statement ends at an unquoted ";",
	// not at a newline, so one entry may span many lines. Cap the total so input
	// of short unterminated lines cannot grow the buffer without bound.
	buf.WriteString(line)
	buf.WriteString(" ")
	if buf.Len() > maxStatementBytes {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("EDS statement exceeds the maximum supported length near line %d", lineNo), nil)
	}
	if !hasSemicolon {
		return nil
	}
	return splitStatements(cur, buf)
}

// splitStatements divides the accumulated buffer on unquoted ";" and adds each
// complete statement, so a line packing several (e.g. "A = 1; B = 2;") yields
// one entry per statement rather than merging them. Content after the last ";"
// is a statement still awaiting its terminator; it is kept in buf to continue
// accumulating on the next line.
func splitStatements(cur **section, buf *strings.Builder) errors.EdgeX {
	text := buf.String()
	buf.Reset()

	start := 0
	inQuote := false
	backslashes := 0
	for i, r := range text {
		switch {
		case isUnescapedQuote(r, backslashes):
			inQuote = !inQuote
		case r == ';' && !inQuote:
			if err := addStatement(cur, strings.TrimSpace(text[start:i])); err != nil {
				return err
			}
			start = i + len(";")
		}
		backslashes = runBackslashes(backslashes, r)
	}
	// Keep any post-";" remainder to continue on the next physical line.
	if rest := strings.TrimSpace(text[start:]); rest != "" {
		buf.WriteString(rest)
		buf.WriteString(" ")
	}
	return nil
}

// handleSectionHeader consumes trimmed if it is a "[...]" section header,
// appending the new section and pointing cur at it. It reports handled=false for
// a non-header line. A header arriving while a statement is still buffered, a
// missing "]", or trailing content after "]" is a contract error.
func handleSectionHeader(e *eds, cur **section, buf *strings.Builder, trimmed string) (handled bool, err errors.EdgeX) {
	if !strings.HasPrefix(trimmed, "[") {
		return false, nil
	}
	// A header while a statement is buffered means the previous statement was
	// never terminated by ";".
	if buf.Len() > 0 && strings.Contains(trimmed, "]") {
		return false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("section header while previous statement is unterminated (missing ';'): %s", truncateForErr(buf.String())), nil)
	}
	if buf.Len() > 0 {
		return false, nil // "[" mid-statement, not a header
	}
	end := strings.Index(trimmed, "]")
	if end < 0 {
		return false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("malformed section header (missing ']'): %s", trimmed), nil)
	}
	// Reject content after "]"; it would be silently dropped otherwise.
	if strings.TrimSpace(trimmed[end+1:]) != "" {
		return false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("unexpected content after section header ']': %s", trimmed), nil)
	}
	sec := &section{name: strings.TrimSpace(trimmed[1:end])}
	e.sections = append(e.sections, sec)
	*cur = sec
	return true, nil
}

// truncateForErr trims s to maxErrMsgLen so a malformed fragment cannot bloat an
// error message; it also collapses leading/trailing space.
func truncateForErr(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxErrMsgLen {
		return s[:maxErrMsgLen] + "…"
	}
	return s
}

// addStatement parses one complete statement and appends it to the current
// section. Empty statements are ignored; a statement with no "=" is a tolerated
// stray token. Shared by the main loop and EOF handling so both paths treat a
// completed statement identically.
func addStatement(cur **section, stmt string) errors.EdgeX {
	if stmt == "" {
		return nil
	}
	en, err := parseEntry(stmt)
	if err != nil {
		return err
	}
	if en == nil {
		return nil // no "=" and not a section — tolerate stray tokens
	}
	if *cur == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("entry outside any section: %s", stmt), nil)
	}
	(*cur).entries = append((*cur).entries, en)
	return nil
}

// scanLine scans one line and reports whether it holds an unquoted ";" (a
// statement terminator) and whether its quotes are left open (unbalanced).
// Because the CIP grammar forbids a string from spanning lines, quote state is
// not carried across lines — so a caller can treat unbalanced as an error and
// each line is scanned independently.
func scanLine(line string) (hasSemicolon, unbalanced bool) {
	inQuote := false
	backslashes := 0
	for _, r := range line {
		switch {
		case isUnescapedQuote(r, backslashes):
			inQuote = !inQuote // flip: entering or leaving a quoted string
		case r == ';' && !inQuote:
			hasSemicolon = true
		}
		backslashes = runBackslashes(backslashes, r)
	}
	return hasSemicolon, inQuote
}

// isUnescapedQuote reports whether r is a double quote that acts as a string
// delimiter — i.e. not escaped by an odd-length run of preceding backslashes.
// backslashes is the count of "\" immediately before r, so a "\\" pair (an
// escaped backslash) leaves the following quote unescaped. Callers use it to
// toggle quote state while scanning.
//
//	isUnescapedQuote('"', 0) // true  (no backslash)
//	isUnescapedQuote('"', 1) // false (\" — escaped)
//	isUnescapedQuote('"', 2) // true  (\\" — escaped backslash, then a delimiter)
func isUnescapedQuote(r rune, backslashes int) bool {
	return r == '"' && backslashes%2 == 0
}

// runBackslashes updates the count of consecutive "\" ending at r: it grows on a
// backslash and resets on anything else, so callers can tell whether the next
// quote is escaped (odd run) or a delimiter (even run).
func runBackslashes(count int, r rune) int {
	if r == '\\' {
		return count + 1
	}
	return 0
}

// parseEntry splits one statement "Keyword = f0, f1, ...;" into its keyword and
// verbatim fields. Returns (nil, nil) if the statement has no "=" (a stray token
// the parser tolerates rather than rejects).
func parseEntry(stmt string) (*entry, errors.EdgeX) {
	stmt = strings.TrimRight(stmt, "; \t") // trailing ";" terminates an entry

	eq := strings.Index(stmt, "=")
	if eq < 0 {
		return nil, nil
	}
	keyword := strings.TrimSpace(stmt[:eq])
	if keyword == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("entry with empty keyword: %s", stmt), nil)
	}

	return &entry{keyword: keyword, fields: splitFields(stmt[eq+1:])}, nil
}

// splitFields splits a field list on commas that fall outside quotes, trimming
// each field but preserving empty slots. "0,,,0xC1" -> ["0","","","0xC1"];
// "\"a, b\", c" -> ["a, b", "c"] (a comma inside quotes is part of the field).
func splitFields(s string) []string {
	var fields []string
	var field strings.Builder
	inQuote := false
	backslashes := 0

	// appendField finalizes the accumulated field: trim surrounding whitespace,
	// then strip ONE pair of delimiting quotes (not every quote), so a field whose
	// content ends in an escaped quote — e.g. "a\"" -> a\" — keeps that quote.
	appendField := func() {
		text := strings.TrimSpace(field.String())
		if len(text) >= 2 && text[0] == '"' && text[len(text)-1] == '"' {
			text = text[1 : len(text)-1]
		}
		fields = append(fields, text)
		field.Reset()
	}

	for _, r := range s {
		switch {
		case isUnescapedQuote(r, backslashes):
			inQuote = !inQuote
			field.WriteRune(r)
		case r == ',' && !inQuote: // separator: end the current field
			appendField()
		default:
			field.WriteRune(r)
		}
		backslashes = runBackslashes(backslashes, r)
	}
	appendField() // the text after the last comma is the final field
	return fields
}

// stripComment removes a "$" comment (to end of line), ignoring "$" inside a
// quoted string. A '"' preceded by '\' is an escaped quote and does not open or
// close a string. EDS uses "$" as its line-comment marker.
func stripComment(line string) string {
	inQuote := false
	backslashes := 0
	for i, r := range line {
		switch {
		case isUnescapedQuote(r, backslashes):
			inQuote = !inQuote
		case r == '$' && !inQuote:
			return line[:i]
		}
		backslashes = runBackslashes(backslashes, r)
	}
	return line
}
