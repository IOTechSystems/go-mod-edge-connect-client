// Package tia2xrt converts Siemens TIA Portal V16 data block SCL source
// exports to XRT S7 device profile JSON files.
//
// Supported TIA types:
//   - Scalar: Bool, SInt, USInt, Byte, Char, Int, UInt, Word, DInt, UDInt,
//     DWord, LInt, ULInt, LWord, Real, LReal, Time
//   - String[n]
//   - Array[lo..hi] of <scalar | Bool | String[n]>
//   - Struct / END_STRUCT (members flattened with dot-notation names)
//   - IEC timers: TON, TOF, TP, TONR (IEC_Timer)
//   - IEC counters: IEC_*COUNTER variants
//
// Only non-optimised (standard-access) data blocks produce correct byte offsets.
package tia2xrt

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ─── Type mapping tables ──────────────────────────────────────────────────────

type scalarInfo struct {
	xrtType   string
	byteSize  int
	alignment int // 0 = any byte boundary, 2 = word (even-byte) boundary
}

// Bool is special: byteSize=0, alignment=0 (bit-packed, handled separately).
var scalarTypes = map[string]scalarInfo{
	"BOOL":  {"bool", 0, 0},
	"SINT":  {"int8", 1, 0},
	"USINT": {"uint8", 1, 0},
	"BYTE":  {"uint8", 1, 0},
	"CHAR":  {"uint8", 1, 0},
	"INT":   {"int16", 2, 2},
	"UINT":  {"uint16", 2, 2},
	"WORD":  {"uint16", 2, 2},
	"WCHAR": {"wchar", 2, 2},
	"DINT":  {"int32", 4, 2},
	"UDINT": {"uint32", 4, 2},
	"DWORD": {"uint32", 4, 2},
	"TIME":  {"int32", 4, 2},
	"TOD":   {"int32", 4, 2},
	"LINT":  {"int64", 8, 2},
	"ULINT": {"uint64", 8, 2},
	"LWORD": {"uint64", 8, 2},
	"LTOD":  {"uint64", 8, 2},
	"REAL":  {"float32", 4, 2},
	"LREAL": {"float64", 8, 2},
}

var arrayTypes = map[string]string{
	"int8":    "int8array",
	"uint8":   "uint8array",
	"int16":   "int16array",
	"uint16":  "uint16array",
	"int32":   "int32array",
	"uint32":  "uint32array",
	"int64":   "int64array",
	"uint64":  "uint64array",
	"float32": "float32array",
	"float64": "float64array",
	"bool":    "boolarray",
}

// skipTypes maps unmappable TIA types to their byte sizes for offset tracking.
var skipTypes = map[string]int{
	"DATE":          2,
	"TIME_OF_DAY":   4,
	"DATE_AND_TIME": 8,
	"DT":            8,
	"LTIME":         8,
	"LDT":           12,
	"HW_DEVICE":     2,
}

var iecTimerNames = map[string]bool{
	"TON": true, "TOF": true, "TP": true, "TONR": true, "IEC_TIMER": true,
}

const iecTimerSize = 16

type iecCounterInfo struct {
	valueType string
	byteSize  int
}

var iecCounterTypes = map[string]iecCounterInfo{
	"IEC_SCOUNTER":  {"int8", 4},
	"IEC_COUNTER":   {"int16", 6},
	"IEC_DCOUNTER":  {"int32", 12},
	"IEC_LCOUNTER":  {"int64", 18},
	"IEC_USCOUNTER": {"uint8", 4},
	"IEC_UCOUNTER":  {"uint16", 6},
	"IEC_UDCOUNTER": {"uint32", 12},
	"IEC_ULCOUNTER": {"uint64", 18},
}

// ─── Parsed variable model ────────────────────────────────────────────────────

// TIAVar represents one variable or struct member from an SCL VAR section.
type TIAVar struct {
	Name           string
	TIAType        string   // normalised to uppercase
	StringLen      int      // for STRING[n]; 0 means use default (254)
	ArrayLo        int      // for ARRAY
	ArrayHi        int      // for ARRAY
	ArrayElemType  string   // element type (upper), for ARRAY
	ArrayStringLen int      // for Array[..] of String[n]; 0 means default (254)
	Children       []TIAVar // for STRUCT
	Comment        string
}

// ─── Output profile types ─────────────────────────────────────────────────────

// ResourceAttributes holds the XRT S7 resource address attributes.
type ResourceAttributes struct {
	Type        string `json:"type"`
	DBNumber    int    `json:"DB_number"`
	Start       int    `json:"start"`
	BitIndex    *int   `json:"bitIndex,omitempty"`
	ArraySize   *int   `json:"array_size,omitempty"`
	StringSize  *int   `json:"size,omitempty"`
	CounterType string `json:"counterType,omitempty"`
}

// ResourceProperties holds the XRT resource read/write and value type.
type ResourceProperties struct {
	ReadWrite string `json:"readWrite"`
	ValueType string `json:"valueType"`
}

// DeviceResource is one XRT S7 device resource entry.
type DeviceResource struct {
	Name        string             `json:"name"`
	Attributes  ResourceAttributes `json:"attributes"`
	Properties  ResourceProperties `json:"properties"`
	Description string             `json:"description,omitempty"`
}

// Profile is the top-level XRT v2 device profile.
type Profile struct {
	Name            string           `json:"name"`
	APIVersion      string           `json:"apiVersion"`
	Labels          []string         `json:"labels"`
	DeviceResources []DeviceResource `json:"deviceResources"`
}

// ─── Internal resource model ──────────────────────────────────────────────────

type s7Resource struct {
	name        string
	xrtType     string
	valueType   string
	dbNumber    int
	start       int
	bitIndex    *int
	arraySize   *int
	stringSize  *int
	counterType string
	readWrite   string
	description string
}

func (r s7Resource) toDeviceResource() DeviceResource {
	rw := r.readWrite
	if rw == "" {
		rw = "RW"
	}
	return DeviceResource{
		Name: r.name,
		Attributes: ResourceAttributes{
			Type:        r.xrtType,
			DBNumber:    r.dbNumber,
			Start:       r.start,
			BitIndex:    r.bitIndex,
			ArraySize:   r.arraySize,
			StringSize:  r.stringSize,
			CounterType: r.counterType,
		},
		Properties:  ResourceProperties{ReadWrite: rw, ValueType: r.valueType},
		Description: r.description,
	}
}

// ─── Compiled regexes ─────────────────────────────────────────────────────────

var (
	reBlockComment    = regexp.MustCompile(`(?s)\(\*.*?\*\)`)
	reAttrBlock       = regexp.MustCompile(`\{[^}]*\}`)
	reDataBlock       = regexp.MustCompile(`(?i)^DATA_BLOCK\s+"([^"]+)"`)
	reOptimizedTrue   = regexp.MustCompile(`(?i)S7_Optimized_Access\s*:=\s*'TRUE'`)
	reOptimizedFalse  = regexp.MustCompile(`(?i)S7_Optimized_Access\s*:=\s*'FALSE'`)
	reStructLine      = regexp.MustCompile(`(?i)^\s*STRUCT\b`)
	reVarDecl         = regexp.MustCompile(`(?i)^([^\s:]+)\s*:\s*(.+)`)
	reArraySig        = regexp.MustCompile(`(?i)^ARRAY\s*\[\s*(-?\d+)\s*\.\.\s*(-?\d+)\s*\]\s+OF\s+(.+)`)
	reStringLen       = regexp.MustCompile(`(?i)^STRING\s*\[\s*(\d+)\s*\]`)
	reWStringLen      = regexp.MustCompile(`(?i)^WSTRING\s*\[\s*(\d+)\s*\]`)
	reInitValue       = regexp.MustCompile(`:=.*$`)
	reSanitizeQuote   = regexp.MustCompile(`"`)
	reSanitizeInvalid = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	reProfileName     = regexp.MustCompile(`[^a-zA-Z0-9_\-]`)
)

// ─── Helper functions ─────────────────────────────────────────────────────────

func intPtr(i int) *int { return &i }

func splitComment(line string) (code, comment string) {
	i := strings.Index(line, "//")
	if i < 0 {
		return strings.TrimRight(line, " \t\r"), ""
	}
	return strings.TrimRight(line[:i], " \t\r"), strings.TrimSpace(line[i+2:])
}

func stripAttrBlocks(s string) string {
	return strings.TrimSpace(reAttrBlock.ReplaceAllString(s, ""))
}

// sanitize removes double-quotes and replaces characters invalid in XRT
// resource names with underscores.
func sanitize(name string) string {
	s := reSanitizeQuote.ReplaceAllString(name, "")
	return reSanitizeInvalid.ReplaceAllString(s, "_")
}

func parseArraySig(typeStr string) (lo, hi int, elemType string, stringLen int, ok bool) {
	m := reArraySig.FindStringSubmatch(strings.TrimSpace(typeStr))
	if m == nil {
		return
	}
	lo, _ = strconv.Atoi(m[1])
	hi, _ = strconv.Atoi(m[2])
	elemRaw := strings.TrimSpace(m[3])
	if sm := reStringLen.FindStringSubmatch(elemRaw); sm != nil {
		slen, _ := strconv.Atoi(sm[1])
		return lo, hi, "STRING", slen, true
	}
	return lo, hi, strings.ToUpper(elemRaw), 0, true
}

// ─── S7 memory-layout offset tracker ─────────────────────────────────────────

// offsetTracker tracks the byte + bit cursor for S7 non-optimised DB layout.
//
// Rules:
//   - Bool variables are bit-packed: up to 8 per byte, LSB first.
//   - Any non-Bool type ends the current bool-byte and starts at the next whole byte.
//   - Types of 2+ bytes must start at an even (word) address.
//   - Structs end with word-alignment padding.
//   - String[n] occupies (2 + n) bytes, padded to an even number.
type offsetTracker struct {
	Byte int
	Bit  int // 0–7
}

func (o *offsetTracker) closeBoolByte() {
	if o.Bit > 0 {
		o.Byte++
		o.Bit = 0
	}
}

func (o *offsetTracker) wordAlign() {
	if o.Byte%2 != 0 {
		o.Byte++
	}
}

func (o *offsetTracker) allocBool() (byteOff, bitIdx int) {
	byteOff, bitIdx = o.Byte, o.Bit
	o.Bit++
	if o.Bit == 8 {
		o.Bit = 0
		o.Byte++
	}
	return
}

func (o *offsetTracker) alloc(size, align int) int {
	o.closeBoolByte()
	if align >= 2 {
		o.wordAlign()
	}
	out := o.Byte
	o.Byte += size
	return out
}

func (o *offsetTracker) allocString(maxLen int) int {
	o.closeBoolByte()
	o.wordAlign()
	out := o.Byte
	total := 2 + maxLen
	o.Byte += total + (total % 2) // pad to even
	return out
}

func (o *offsetTracker) allocWString(maxLen int) int {
	o.closeBoolByte()
	o.wordAlign()
	out := o.Byte
	o.Byte += (2 + maxLen) * 2
	return out
}

func (o *offsetTracker) closeStruct() {
	o.closeBoolByte()
	o.wordAlign()
}

// ─── SCL parser ───────────────────────────────────────────────────────────────

func parseVarBlock(lines []string, pos int) ([]TIAVar, int) {
	var variables []TIAVar

	for pos < len(lines) {
		raw := lines[pos]
		pos++

		code, comment := splitComment(raw)
		code = strings.TrimSpace(stripAttrBlocks(code))
		if code == "" {
			continue
		}

		upper := strings.TrimSpace(strings.ToUpper(strings.TrimRight(code, ";")))
		if upper == "END_STRUCT" || upper == "BEGIN" || upper == "END_DATA_BLOCK" {
			break
		}
		if strings.HasPrefix(upper, "BEGIN") || strings.HasPrefix(upper, "END_DATA_BLOCK") {
			break
		}

		m := reVarDecl.FindStringSubmatch(code)
		if m == nil {
			continue
		}

		varName := m[1]
		typeRaw := strings.TrimSpace(strings.TrimRight(m[2], ";"))
		typeRaw = strings.TrimSpace(reInitValue.ReplaceAllString(typeRaw, ""))
		typeRaw = strings.TrimSpace(stripAttrBlocks(typeRaw))
		upperType := strings.ToUpper(typeRaw)

		if upperType == "STRUCT" {
			children, newPos := parseVarBlock(lines, pos)
			pos = newPos
			variables = append(variables, TIAVar{
				Name: varName, TIAType: "STRUCT",
				Children: children, Comment: comment,
			})
			continue
		}

		if lo, hi, elemType, slen, ok := parseArraySig(typeRaw); ok {
			variables = append(variables, TIAVar{
				Name: varName, TIAType: "ARRAY",
				ArrayLo: lo, ArrayHi: hi,
				ArrayElemType: elemType, ArrayStringLen: slen,
				Comment: comment,
			})
			continue
		}

		if sm := reStringLen.FindStringSubmatch(typeRaw); sm != nil {
			slen, _ := strconv.Atoi(sm[1])
			variables = append(variables, TIAVar{
				Name: varName, TIAType: "STRING",
				StringLen: slen, Comment: comment,
			})
			continue
		}

		if wm := reWStringLen.FindStringSubmatch(typeRaw); wm != nil {
			wslen, _ := strconv.Atoi(wm[1])
			variables = append(variables, TIAVar{
				Name: varName, TIAType: "WSTRING",
				StringLen: wslen, Comment: comment,
			})
			continue
		}

		variables = append(variables, TIAVar{Name: varName, TIAType: upperType, Comment: comment})
	}

	return variables, pos
}

// ParseSCL parses a TIA Portal V16 SCL data block source.
// Returns the block name, whether optimized access is enabled, and the parsed variables.
func ParseSCL(text string) (blockName string, isOptimized bool, variables []TIAVar) {
	text = reBlockComment.ReplaceAllString(text, " ")
	lines := strings.Split(text, "\n")
	blockName = "UnknownBlock"

	for pos := 0; pos < len(lines); pos++ {
		code, _ := splitComment(lines[pos])
		code = strings.TrimSpace(code)

		if m := reDataBlock.FindStringSubmatch(code); m != nil {
			blockName = m[1]
			continue
		}
		if reOptimizedTrue.MatchString(code) {
			isOptimized = true
		} else if reOptimizedFalse.MatchString(code) {
			isOptimized = false
		}
		if reStructLine.MatchString(code) {
			variables, _ = parseVarBlock(lines, pos+1)
			break
		}
	}
	return
}

// ─── Variable → resource flattener ───────────────────────────────────────────

func flatten(
	variables []TIAVar,
	off *offsetTracker,
	dbNumber int,
	prefix string,
	resources *[]s7Resource,
	warnings *[]string,
) {
	for _, v := range variables {
		full := prefix + v.Name
		name := sanitize(full)
		ttype := v.TIAType

		if ttype == "STRUCT" {
			flatten(v.Children, off, dbNumber, full+".", resources, warnings)
			off.closeStruct()
			continue
		}

		if ttype == "ARRAY" {
			count := v.ArrayHi - v.ArrayLo + 1
			elem := v.ArrayElemType
			switch elem {
			case "STRUCT":
				*warnings = append(*warnings,
					fmt.Sprintf("'%s': Array of Struct is not supported – skipped.", full))
			case "STRING":
				slen := v.ArrayStringLen
				if slen == 0 {
					slen = 254
				}
				for i := 0; i < count; i++ {
					start := off.allocString(slen)
					*resources = append(*resources, s7Resource{
						name: fmt.Sprintf("%s_%d", name, i), xrtType: "DB",
						valueType: "string", dbNumber: dbNumber,
						start: start, stringSize: intPtr(slen),
						description: v.Comment,
					})
				}
			case "BOOL":
				for i := 0; i < count; i++ {
					b, bit := off.allocBool()
					*resources = append(*resources, s7Resource{
						name: fmt.Sprintf("%s_%d", name, i), xrtType: "DB",
						valueType: "bool", dbNumber: dbNumber,
						start: b, bitIndex: intPtr(bit),
						description: v.Comment,
					})
				}
			default:
				scalar, ok := scalarTypes[elem]
				if !ok {
					*warnings = append(*warnings, fmt.Sprintf(
						"'%s': Unsupported array element type '%s' – skipped. "+
							"Offsets after this point may be incorrect.", full, elem))
					continue
				}
				arrVT, ok := arrayTypes[scalar.xrtType]
				if !ok {
					*warnings = append(*warnings, fmt.Sprintf(
						"'%s': No XRT array type for '%s' – skipped.", full, scalar.xrtType))
					continue
				}
				start := off.alloc(scalar.byteSize*count, 2)
				*resources = append(*resources, s7Resource{
					name: name, xrtType: "DB",
					valueType: arrVT, dbNumber: dbNumber,
					start: start, arraySize: intPtr(count),
					description: v.Comment,
				})
			}
			continue
		}

		if ttype == "STRING" {
			slen := v.StringLen
			if slen == 0 {
				slen = 254
			}
			start := off.allocString(slen)
			*resources = append(*resources, s7Resource{
				name: name, xrtType: "DB",
				valueType: "string", dbNumber: dbNumber,
				start: start, stringSize: intPtr(slen),
				description: v.Comment,
			})
			continue
		}

		if ttype == "WSTRING" {
			slen := v.StringLen
			if slen == 0 {
				slen = 254
			}
			start := off.allocWString(slen)
			*resources = append(*resources, s7Resource{
				name: name, xrtType: "DB",
				valueType: "wstring", dbNumber: dbNumber,
				start: start, stringSize: intPtr(slen),
				description: v.Comment,
			})
			continue
		}

		if iecTimerNames[ttype] {
			start := off.alloc(iecTimerSize, 2)
			*resources = append(*resources, s7Resource{
				name: name, xrtType: "IEC_Timer",
				valueType: "object", dbNumber: dbNumber,
				start: start, description: v.Comment,
			})
			continue
		}

		if ctr, ok := iecCounterTypes[ttype]; ok {
			start := off.alloc(ctr.byteSize, 2)
			*resources = append(*resources, s7Resource{
				name: name, xrtType: "IEC_Counter",
				valueType: "object", dbNumber: dbNumber,
				start: start, counterType: ctr.valueType,
				description: v.Comment,
			})
			continue
		}

		if skipSz, ok := skipTypes[ttype]; ok {
			*warnings = append(*warnings, fmt.Sprintf(
				"'%s' (%s): not supported in XRT S7 – skipped (byte offset advanced by %d).",
				full, ttype, skipSz))
			off.alloc(skipSz, 2)
			continue
		}

		if ttype == "BOOL" {
			b, bit := off.allocBool()
			*resources = append(*resources, s7Resource{
				name: name, xrtType: "DB",
				valueType: "bool", dbNumber: dbNumber,
				start: b, bitIndex: intPtr(bit),
				description: v.Comment,
			})
			continue
		}

		if scalar, ok := scalarTypes[ttype]; ok {
			start := off.alloc(scalar.byteSize, scalar.alignment)
			*resources = append(*resources, s7Resource{
				name: name, xrtType: "DB",
				valueType: scalar.xrtType, dbNumber: dbNumber,
				start: start, description: v.Comment,
			})
			continue
		}

		*warnings = append(*warnings, fmt.Sprintf(
			"'%s': Unknown type '%s' – skipped. Offsets for subsequent variables may be incorrect.",
			full, v.TIAType))
	}
}

// ─── Profile builder ─────────────────────────────────────────────────────────

// BuildProfile converts parsed TIA variables into an XRT v2 device profile.
// Returns the profile and any warnings generated during conversion.
func BuildProfile(name string, dbNumber int, variables []TIAVar) (Profile, []string) {
	off := &offsetTracker{}
	var resources []s7Resource
	var warnings []string
	flatten(variables, off, dbNumber, "", &resources, &warnings)

	deviceResources := make([]DeviceResource, 0, len(resources))
	for _, r := range resources {
		deviceResources = append(deviceResources, r.toDeviceResource())
	}

	return Profile{
		Name:            name,
		APIVersion:      "v2",
		Labels:          []string{},
		DeviceResources: deviceResources,
	}, warnings
}

// Convert is a convenience function that parses an SCL string and builds a profile
// in one call. If the block uses optimized access and allowOptimized is false,
// an error is returned.
//
// profileName overrides the block name extracted from the source; pass "" to
// derive it automatically (invalid characters replaced with underscores).
func Convert(sclText string, dbNumber int, profileName string, allowOptimized bool) (Profile, []string, error) {
	blockName, isOptimized, variables := ParseSCL(sclText)

	if isOptimized && !allowOptimized {
		return Profile{}, nil, fmt.Errorf(
			"block has S7_Optimized_Access := 'TRUE'; " +
				"disable optimized access in TIA Portal or pass allowOptimized=true")
	}

	name := profileName
	if name == "" {
		name = reProfileName.ReplaceAllString(blockName, "_")
	}

	profile, warnings := BuildProfile(name, dbNumber, variables)

	if isOptimized {
		warnings = append([]string{"optimized-access block – byte offsets will be incorrect"}, warnings...)
	}
	return profile, warnings, nil
}

// MarshalProfile serialises a Profile to indented JSON.
func MarshalProfile(p Profile) ([]byte, error) {
	return json.MarshalIndent(p, "", "  ")
}
