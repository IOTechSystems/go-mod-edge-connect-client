// Copyright (C) 2026 IOTech Ltd

// Package tia converts a Siemens TIA Portal data block SCL source export into an
// EdgeX DeviceProfile for the XRT S7 device service. Its Convert function
// satisfies profileconv.ConvertFunc.
//
// The export carries no addresses, so every offset is derived by accumulating
// type sizes in declaration order — which only holds for a non-optimised
// (standard access) block. Optimized blocks are rejected: TIA Portal lays them
// out internally, so a derived address would be wrong. A type of unknown size
// aborts the conversion for the same reason; one XRT cannot represent emits no
// resource but still advances the cursor. See s7types.go for which is which.
//
// This file reads in execution order: Convert, parseSCL, buildProfile, then the
// flattener. The offset cursor is in offset.go.
package tia

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv"
)

// Input limits. A DB source file is untrusted input: without these, a few hundred
// bytes of hostile text can demand gigabytes of resources.
const (
	// defaultStringLen is what S7 assumes when String is declared without a
	// length.
	defaultStringLen = 254
	// maxStringLen is S7's String limit, applied to WString too. WString allows up
	// to 16382, but it is skipped rather than emitted, so the tighter cap only
	// bounds how far the cursor can advance.
	maxStringLen = 254
	// Canonical TIAType values. The parser sets these and the flattener
	// dispatches on them, so a typo in either half would silently drop a whole
	// category of declaration rather than fail to compile.
	typeStruct  = "STRUCT"
	typeArray   = "ARRAY"
	typeString  = "STRING"
	typeWString = "WSTRING" // one WORD per character, so twice a String's size
	typeBool    = "BOOL"

	// skippedSuffix ends every warning about a declaration the parser could not
	// measure. Skipping leaves the cursor short of the bytes the declaration
	// occupies, so nothing after it can be trusted.
	skippedSuffix = " – skipped. Offsets after this point may be incorrect."

	// unknownBlockName is the placeholder blockName before a DATA_BLOCK header is
	// seen. Such a source yields no resources and is rejected, so this never
	// names a returned profile — seeing it means the header was missed.
	unknownBlockName = "UnknownBlock"

	// noStringLen marks "no length declared", so that an explicit String[0]
	// (legal in TIA: header only, no capacity) is not treated as absent.
	noStringLen = -1
	// maxStructDepth bounds parseVarBlock's recursion into nested STRUCT bodies.
	maxStructDepth = 64
	// maxLineBytes caps one declaration line, matching the eds parser.
	maxLineBytes = 1 << 20
	// maxArrayElements caps one array declaration: Bool and String arrays allocate
	// per element, so the count is the amplification vector.
	maxArrayElements = 100_000
)

// Option keys for Convert's options map.
const (
	// OptionDBNumber sets the data block number (int, default 1). It is a TIA
	// project property, absent from the export, so the caller must supply it.
	OptionDBNumber = "dbNumber"
	// OptionProfileName overrides the profile name; the block name is used
	// otherwise. Either way invalid characters are replaced.
	OptionProfileName = "profileName"
)

var _ profileconv.ConvertFunc = Convert

// Convert turns one TIA Portal data block SCL source export into an EdgeX
// DeviceProfile.
//
//	var convert profileconv.ConvertFunc = tia.Convert
//	profile, err := convert(ctx, lc, data, map[string]any{tia.OptionDBNumber: 5})
//
// ctx is unused: the conversion is a synchronous in-memory pass.
func Convert(ctx context.Context, lc logger.LoggingClient,
	data []byte, options map[string]any) (dtos.DeviceProfile, errors.EdgeX) {
	var profile dtos.DeviceProfile

	dbNumber, profileName, edgexErr := parseOptions(options)
	if edgexErr != nil {
		return profile, edgexErr
	}

	// TIA Portal writes a UTF-8 BOM. Left in place it defeats the anchored
	// DATA_BLOCK match, which yields no resources — and, worse, hides an
	// optimized-access block behind that generic failure.
	src, edgexErr := parseSCL(lc, strings.TrimPrefix(string(data), "\ufeff"))
	if edgexErr != nil {
		return profile, edgexErr
	}
	if edgexErr := checkConvertible(src); edgexErr != nil {
		return profile, edgexErr
	}

	name := resolveProfileName(profileName, src.blockName)
	profile, edgexErr = buildProfile(lc, name, dbNumber, src)
	if edgexErr != nil {
		return profile, edgexErr
	}

	// A named profile with no resources is not convertible content: a caller
	// could not tell it apart from a successful conversion.
	if len(profile.DeviceResources) == 0 {
		return profile, errors.NewCommonEdgeX(errors.KindContractInvalid,
			"DB source produced no device resources", nil)
	}

	// Validate covers the required fields, the value-type enums and duplicate
	// names.
	if verr := profile.Validate(); verr != nil {
		return profile, errors.NewCommonEdgeXWrapper(verr)
	}
	return profile, nil
}

// ─── Parsed variable model ────────────────────────────────────────────────────

// tiaVar represents one variable or struct member from an SCL VAR section.
type tiaVar struct {
	Name           string
	TIAType        string // normalised to uppercase
	StringLen      int    // -1 where the declaration gave no length
	ArrayLo        int
	ArrayHi        int
	ArrayElemType  string   // normalised to uppercase
	ArrayStringLen int      // -1 where the element type declared no length
	Children       []tiaVar // for STRUCT
	Comment        string
	// ReadWrite is R when the declaration marks the variable unwritable for
	// external clients, RW otherwise.
	ReadWrite string
}

// ─── Compiled regexes ─────────────────────────────────────────────────────────

var (
	reBlockComment = regexp.MustCompile(`(?s)\(\*.*?\*\)`)
	reAttrBlock    = regexp.MustCompile(`\{[^}]*\}`)
	// TIA marks a variable read-only for external clients with
	// ExternalWritable := 'False' inside the attribute block. XRT reads a DB over
	// the same "external client" route as HMI and OPC UA, so this is exactly the
	// resource's readWrite.
	reExternalWritableFalse = regexp.MustCompile(`(?i)ExternalWritable\s*:=\s*'False'`)
	reDataBlock             = regexp.MustCompile(`(?i)^DATA_BLOCK\s+"([^"]+)"`)
	reTypeBlock             = regexp.MustCompile(`(?i)^TYPE\s+"?([^"\s]+)"?`)
	reOptimizedTrue         = regexp.MustCompile(`(?i)S7_Optimized_Access\s*:=\s*'TRUE'`)
	reOptimizedFalse        = regexp.MustCompile(`(?i)S7_Optimized_Access\s*:=\s*'FALSE'`)
	reStructLine            = regexp.MustCompile(`(?i)^\s*STRUCT\b`)
	// A quoted name may contain spaces and colons ("Motor Speed", "Tank:Level"),
	// which is what TIA quoting is for, so match the quoted form first — otherwise
	// a colon inside the name is taken as the name/type separator.
	reVarDecl         = regexp.MustCompile(`(?i)^("[^"]*"|[^\s:]+)\s*:\s*(.+)`)
	reArraySig        = regexp.MustCompile(`(?i)^ARRAY\s*\[\s*(-?\d+)\s*\.\.\s*(-?\d+)\s*\]\s+OF\s+(.+)`)
	reStringLen       = regexp.MustCompile(`(?i)^STRING\s*\[\s*(\d+)\s*\]`)
	reWStringLen      = regexp.MustCompile(`(?i)^WSTRING\s*\[\s*(\d+)\s*\]`)
	reInitValue       = regexp.MustCompile(`:=.*$`)
	reSanitizeQuote   = regexp.MustCompile(`"`)
	reSanitizeInvalid = regexp.MustCompile(`[^a-zA-Z0-9_]`)
	reProfileName     = regexp.MustCompile(`[^a-zA-Z0-9_\-]`)
)

// ─── SCL parser ───────────────────────────────────────────────────────────────

// sclSource is what one parse pass extracts.
type sclSource struct {
	blockName   string
	isOptimized bool
	variables   []tiaVar
	// udts maps a UDT name to its member list, unresolved: a UDT may reference
	// another and the export order is not topological, so nothing can be measured
	// until every definition is collected.
	udts map[string][]tiaVar
	// blockCount detects a file holding more than one data block, which is
	// rejected rather than silently reduced to the first.
	blockCount int
}

// blockKind is which top-level block the scan is inside. Naming the type keeps an
// ordinary int from being passed where one of these is meant.
type blockKind int

const (
	outside blockKind = iota
	inDataBlock
	inType
)

// parseSCL reads the data block and any UDT definitions the file carries.
//
// Both use STRUCT for their declaration block and definitions usually come
// first, so the parser tracks which top-level block it is inside: keying off the
// first STRUCT would build the profile from a UDT body instead.
func parseSCL(lc logger.LoggingClient, text string) (sclSource, errors.EdgeX) {
	lines := strings.Split(stripBlockComments(text), "\n")

	src := sclSource{blockName: unknownBlockName, udts: map[string][]tiaVar{}}

	// The name from the last TYPE header, used to key the UDT once its STRUCT is
	// read.
	udtName := ""
	// Which block the next STRUCT belongs to. Reset once that STRUCT is read, so a
	// second one in the same block is not taken for the block's own body.
	structOwner := outside
	// Which block an attribute line belongs to. Kept until the next block header,
	// because TIA writes a pragma block over several lines.
	attrOwner := outside

	for pos := 0; pos < len(lines); pos++ {
		code, _ := splitComment(lines[pos])
		code = strings.TrimSpace(code)

		if header, name := blockHeader(code); header != outside {
			if header == inDataBlock {
				src.blockCount++
				src.blockName = name
			} else {
				udtName = name
			}
			structOwner, attrOwner = header, header
			continue
		}

		// Only the data block's own flag counts. A UDT is always standard access,
		// so letting its attribute block through would let a trailing TYPE clear a
		// genuine TRUE — and re-exporting with dependent blocks, which is what the
		// missing-UDT error tells users to do, produces exactly that file.
		if attrOwner == inDataBlock {
			src.isOptimized = optimizedFlag(code, src.isOptimized)
		}

		if !reStructLine.MatchString(code) {
			continue
		}
		members, newPos, err := parseVarBlock(lc, lines, pos+1, 1)
		pos = newPos - 1
		if err != nil {
			return src, err
		}
		src.storeMembersIn(lc, structOwner, udtName, members)
		structOwner = outside
	}
	return src, nil
}

func (src *sclSource) storeMembersIn(lc logger.LoggingClient, owner blockKind,
	udtName string, members []tiaVar) {
	switch owner {
	case inDataBlock:
		src.variables = members
	case inType:
		src.udts[strings.ToUpper(udtName)] = members
	default:
		// A STRUCT outside any DATA_BLOCK or TYPE, or a second one within either.
		// Its members belong to no block, so they are dropped — but silently
		// dropping them would hide a whole section of a hand-edited export.
		lc.Warnf("tia: a STRUCT of %d declaration(s) is outside any DATA_BLOCK "+
			"or TYPE – ignored.", len(members))
	}
}

// blockHeader reports which top-level block a line opens, and its name. It
// returns outside for any other line.
func blockHeader(code string) (blockKind, string) {
	if m := reDataBlock.FindStringSubmatch(code); m != nil {
		return inDataBlock, m[1]
	}
	if m := reTypeBlock.FindStringSubmatch(code); m != nil {
		return inType, m[1]
	}
	return outside, ""
}

// optimizedFlag applies an S7_Optimized_Access attribute, keeping the current
// value for a line that carries neither spelling.
func optimizedFlag(code string, current bool) bool {
	switch {
	case reOptimizedTrue.MatchString(code):
		return true
	case reOptimizedFalse.MatchString(code):
		return false
	}
	return current
}

// parseVarBlock reads declarations until the block ends, returning them and the
// line after it. It recurses for a nested STRUCT, so depth bounds that recursion.
func parseVarBlock(lc logger.LoggingClient, lines []string, pos, depth int) ([]tiaVar, int, errors.EdgeX) {
	var variables []tiaVar

	for pos < len(lines) {
		raw := lines[pos]
		pos++

		// Skipping the line would leave the cursor short of the bytes the
		// declaration occupies, making every later address wrong. The eds parser
		// rejects an over-long line for the same reason.
		if len(raw) > maxLineBytes {
			return variables, pos, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
				"line %d is longer than %d bytes, so the bytes it occupies are unknown "+
					"and every later address would be wrong", pos, maxLineBytes), nil)
		}

		code, comment := splitComment(raw)
		// Read the attribute block before discarding it: it is the only place the
		// declaration states whether an external client may write the variable.
		readWrite := readWriteFor(code)
		code = strings.TrimSpace(stripAttrBlocks(code))
		if code == "" {
			continue
		}

		upper := strings.TrimSpace(strings.ToUpper(strings.TrimRight(code, ";")))
		if upper == "END_STRUCT" || upper == "END_TYPE" || upper == "BEGIN" || upper == "END_DATA_BLOCK" {
			break
		}

		// Skipping it would leave the cursor short of whatever bytes it occupies,
		// so every later address would be wrong while the profile still looked
		// valid. A comma-separated list (a, b : Int) lands here.
		m := reVarDecl.FindStringSubmatch(code)
		if m == nil {
			return variables, pos, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
				"unparsable declaration %q; declare one variable per line",
				strings.TrimSpace(code)), nil)
		}

		v, newPos, ok, err := parseDecl(lc, m[1], m[2], lines, pos, depth)
		pos = newPos
		if err != nil {
			return variables, pos, err
		}
		if !ok {
			continue
		}
		v.Comment, v.ReadWrite = comment, readWrite
		variables = append(variables, v)
	}

	return variables, pos, nil
}

// checkConvertible rejects sources whose offsets cannot be derived at all.
func checkConvertible(src sclSource) errors.EdgeX {
	// The DB number comes from options, not the file, so a second block could not
	// be numbered. Rejecting beats silently converting only the first.
	if src.blockCount > 1 {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"the file contains %d DATA_BLOCK sections; export each data block separately",
			src.blockCount), nil)
	}
	// TIA Portal lays an optimized block out internally, so no offset follows from
	// the declaration order. Converting anyway would defer the failure to XRT
	// reading the wrong addresses, so there is deliberately no override.
	if src.isOptimized {
		return errors.NewCommonEdgeX(errors.KindContractInvalid,
			"optimized-access blocks are not supported: byte offsets cannot be derived "+
				"from the declaration order. Turn off 'Optimized block access' in the "+
				"block properties and re-export", nil)
	}
	return nil
}

// resolveProfileName sanitises the override as well as the derived name, which
// would otherwise let a caller-supplied name keep characters the block name loses.
func resolveProfileName(override, blockName string) string {
	name := override
	if name == "" {
		name = blockName
	}
	return reProfileName.ReplaceAllString(name, "_")
}

// parseOptions reads the caller's options, defaulting the DB number to 1 and the
// profile name to empty (meaning: derive it from the source).
func parseOptions(options map[string]any) (dbNumber int, profileName string, err errors.EdgeX) {
	dbNumber = 1
	if v, present := options[OptionDBNumber]; present {
		n, ok := v.(int)
		switch {
		case !ok:
			return 0, "", errors.NewCommonEdgeX(errors.KindContractInvalid,
				fmt.Sprintf("option %q must be an int, got %T", OptionDBNumber, v), nil)
		case n < 0:
			return 0, "", errors.NewCommonEdgeX(errors.KindContractInvalid,
				fmt.Sprintf("option %q must not be negative, got %d", OptionDBNumber, n), nil)
		}
		dbNumber = n
	}
	if v, present := options[OptionProfileName]; present {
		name, ok := v.(string)
		if !ok {
			return 0, "", errors.NewCommonEdgeX(errors.KindContractInvalid,
				fmt.Sprintf("option %q must be a string, got %T", OptionProfileName, v), nil)
		}
		profileName = name
	}
	// A misspelt key would otherwise be ignored and the default used, so a caller
	// asking for DB 5 would silently get a profile addressing DB 1.
	for key := range options {
		if key != OptionDBNumber && key != OptionProfileName {
			return 0, "", errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
				"unknown option %q; supported: %q, %q", key, OptionDBNumber, OptionProfileName), nil)
		}
	}
	return dbNumber, profileName, nil
}

// parseDecl turns one "name : type" declaration into a tiaVar. A STRUCT consumes
// the lines of its own body, so the caller's cursor is returned either way; ok is
// false for a declaration that had to be skipped.
func parseDecl(lc logger.LoggingClient, name, typePart string, lines []string,
	pos, depth int) (tiaVar, int, bool, errors.EdgeX) {
	typeRaw := strings.TrimSpace(strings.TrimRight(typePart, ";"))
	typeRaw = strings.TrimSpace(reInitValue.ReplaceAllString(typeRaw, ""))
	typeRaw = strings.TrimSpace(stripAttrBlocks(typeRaw))
	// Normalise once, so every table lookup downstream sees the canonical
	// spelling (see normaliseType).
	upperType := normaliseType(strings.ToUpper(typeRaw))

	if upperType == typeStruct {
		// Skipping would leave the struct body unconsumed: its stray END_STRUCT
		// lines then unwind the enclosing levels, silently discarding every later
		// declaration, so this has to abort instead.
		if depth >= maxStructDepth {
			return tiaVar{}, pos, false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
				"'%s': STRUCT nesting is deeper than %d, so its body cannot be measured",
				name, maxStructDepth), nil)
		}
		children, newPos, err := parseVarBlock(lc, lines, pos, depth+1)
		return tiaVar{Name: name, TIAType: typeStruct, Children: children}, newPos, true, err
	}

	if lo, hi, elemType, slen, ok := parseArraySig(typeRaw); ok {
		return tiaVar{
			Name: name, TIAType: typeArray,
			ArrayLo: lo, ArrayHi: hi,
			ArrayElemType: elemType, ArrayStringLen: slen,
		}, pos, true, nil
	}

	if sized, lenText, ok := matchSizedString(typeRaw); ok {
		slen, err := strconv.Atoi(lenText)
		if err != nil || slen > maxStringLen {
			lc.Warnf("tia: '%s': %s length %q is out of range"+skippedSuffix, name, typeRaw, lenText)
			return tiaVar{}, pos, false, nil
		}
		return tiaVar{Name: name, TIAType: sized, StringLen: slen}, pos, true, nil
	}

	// A bare String/WString (no [n]) takes the S7 default length; mark it absent
	// rather than 0, so an explicit String[0] stays distinguishable.
	if upperType == typeString || upperType == typeWString {
		return tiaVar{Name: name, TIAType: upperType, StringLen: noStringLen}, pos, true, nil
	}
	return tiaVar{Name: name, TIAType: upperType}, pos, true, nil
}

// ─── Profile builder ─────────────────────────────────────────────────────────

// buildProfile turns the parsed declarations into resources.
func buildProfile(lc logger.LoggingClient, name string, dbNumber int,
	src sclSource) (dtos.DeviceProfile, errors.EdgeX) {
	off := &offsetTracker{}
	var resources []dtos.DeviceResource
	f := &flattener{
		off:       off,
		dbNumber:  dbNumber,
		udts:      src.udts,
		names:     map[string]string{},
		resolving: map[string]bool{},
		sizes:     map[string]int{},
		resources: &resources,
		lc:        lc,
	}
	err := f.walkMembers(src.variables, "")
	if err != nil {
		return dtos.DeviceProfile{}, err
	}

	return dtos.DeviceProfile{
		DeviceProfileBasicInfo: dtos.DeviceProfileBasicInfo{Name: name},
		DeviceResources:        resources,
	}, nil
}

// ─── Variable → resource flattener ───────────────────────────────────────────

// flattener walks the declaration tree, accumulating resources and the offset
// cursor.
type flattener struct {
	off      *offsetTracker
	dbNumber int
	udts     map[string][]tiaVar
	// names maps an emitted resource name to the source path that produced it.
	names map[string]string
	// resolving guards against a UDT cycle (A -> B -> A), which TIA Portal
	// rejects but a hand-edited file can still contain.
	resolving map[string]bool
	// sizes caches each UDT's measured size. A UDT measures the same wherever it
	// is referenced, and without this a type referenced from several places is
	// re-walked once per path — exponential in the depth of a shared chain.
	sizes     map[string]int
	resources *[]dtos.DeviceResource
	lc        logger.LoggingClient
	// quiet suppresses warnings for a pass that only measures a UDT, whose notes
	// would repeat the pass that emits its members.
	quiet bool
}

// walkMembers emits one resource per declaration, in order. prefix carries the
// enclosing struct path, so a nested member becomes parent_child; emit recurses
// back here for a STRUCT, which is why the offset cursor has to be shared state
// rather than a return value.
func (f *flattener) walkMembers(variables []tiaVar, prefix string) errors.EdgeX {
	for _, v := range variables {
		full := prefix + v.Name
		if err := f.reserveName(v, full); err != nil {
			return err
		}
		if err := f.dispatchByType(v, full); err != nil {
			return err
		}
	}
	return nil
}

// reserveName rejects a sanitising collision, naming both sources. Two distinct
// variables can map onto one resource name (the member "s.a" and a top-level
// "s_a" both become s_a); EdgeX rejects the duplicate too, but names only the
// result, leaving the user unsure which declaration to rename.
func (f *flattener) reserveName(v tiaVar, full string) errors.EdgeX {
	if v.TIAType == typeStruct {
		return nil
	}
	name := sanitize(full)
	if other, taken := f.names[name]; taken {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"'%s' and '%s' both sanitise to the resource name %q; rename one of them",
			other, full, name), nil)
	}
	f.names[name] = full
	return nil
}

// dispatchByType routes one declaration to its type category, which allocates its
// offset and, for a type XRT can represent, appends a resource.
//
// The four S7-keyed tables are disjoint (TestDispatchTablesAreDisjoint enforces
// it), so those arms cannot shadow each other. scalarTypes is tried last so the
// typeBool case wins over its BOOL entry (see scalarTypes).
func (f *flattener) dispatchByType(v tiaVar, full string) errors.EdgeX {
	ttype := v.TIAType

	// Table lookups use the comma-ok form, not a zero-size test: a future entry
	// with size 0 must still take its own arm rather than fall through to scalar.
	if ctr, ok := iecCounterTypes[ttype]; ok {
		start := f.off.alloc(ctr.byteSize, 2)
		f.appendResource(v, full, common.ValueTypeObject, start, map[string]any{
			attrType: typeIECCounter, attrCounterType: ctr.counterType,
		}, nil)
		return nil
	}
	if skipSz, ok := skipTypes[ttype]; ok {
		f.warnf("'%s' (%s): not supported in XRT S7 – skipped (offset advanced by %d).",
			full, ttype, skipSz)
		align := 2
		if skipSz == 1 {
			align = 0
		}
		f.off.alloc(skipSz, align)
		return nil
	}

	switch {
	case ttype == typeStruct:
		if err := f.walkMembers(v.Children, full+"."); err != nil {
			return err
		}
		f.off.toWordBoundary()
		return nil

	case ttype == typeArray:
		return f.emitArray(v, full)

	case ttype == typeString:
		slen := stringLenOrDefault(v.StringLen)
		start := f.off.allocString(slen)
		f.appendResource(v, full, common.ValueTypeString, start,
			map[string]any{attrSize: slen}, nil)
		return nil

	// WString is unsupported by XRT (the Checklist marks it so, and it is
	// s1500-only), but it still occupies (2+n)*2 bytes. Emitting it as a String
	// would hand XRT two bytes per character and mis-read every one.
	case ttype == typeWString:
		slen := stringLenOrDefault(v.StringLen)
		f.off.allocWString(slen)
		f.warnf("'%s' (WString): not supported in XRT S7 – skipped (offset advanced by %d).",
			full, (2+slen)*2)
		return nil

	case iecTimerNames[ttype]:
		start := f.off.alloc(iecTimerSize, 2)
		f.appendResource(v, full, common.ValueTypeObject, start,
			map[string]any{attrType: typeIECTimer}, nil)
		return nil

	case ttype == typeBool:
		b, bit := f.off.allocBool()
		f.appendResource(v, full, common.ValueTypeBool, b,
			map[string]any{attrBitIndex: bit}, nil)
		return nil
	}

	if scalar, ok := scalarTypes[ttype]; ok {
		start := f.off.alloc(scalar.byteSize, scalar.alignment)
		f.appendResource(v, full, scalar.valueType, start, nil, scalarMaximum(scalar))
		return nil
	}
	return f.handleUDTReference(v, full)
}

// emitArray handles ARRAY declarations; its element kinds differ enough in layout
// (one resource for the whole array, but three ways of advancing the cursor) that
// each is built separately.
func (f *flattener) emitArray(v tiaVar, full string) errors.EdgeX {
	count := v.ArrayHi - v.ArrayLo + 1
	elem := v.ArrayElemType

	// A reversed range (Array[10..0]) yields a negative count, which walks the
	// cursor backwards and emits a negative array_size that Validate() accepts.
	if count <= 0 || count > maxArrayElements {
		f.warnf("'%s': array element count %d is out of range (1..%d)"+skippedSuffix,
			full, count, maxArrayElements)
		return nil
	}

	switch elem {
	case typeStruct:
		// Neither the resource naming nor the element alignment for an array of
		// structs has any authoritative source, so guessing would emit addresses
		// that look plausible and are wrong.
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"'%s': Array of Struct is not supported; declare the elements individually",
			full), nil)

	case typeString:
		// size is the per-element length, array_size the element count — the shape
		// a working XRT profile uses for a string array.
		slen := stringLenOrDefault(v.ArrayStringLen)
		start := f.off.allocString(slen)
		for i := 1; i < count; i++ {
			f.off.allocString(slen)
		}
		f.appendResource(v, full, common.ValueTypeStringArray, start,
			map[string]any{attrSize: slen, attrArraySize: count}, nil)
		return nil

	case typeWString:
		// Known size per element, so the cursor can advance even though XRT cannot
		// represent the type (see the scalar WString arm).
		slen := stringLenOrDefault(v.ArrayStringLen)
		for i := 0; i < count; i++ {
			f.off.allocWString(slen)
		}
		f.warnf("'%s' (WString array): not supported in XRT S7 – skipped "+
			"(offset advanced by %d).", full, count*(2+slen)*2)
		return nil

	case typeBool:
		// Starts on its own even byte, elements bit-packed from bit 0: the address
		// is that byte with no bitIndex, and array_size says how many bits follow.
		// Closing the byte afterwards is what stops a following scalar Bool from
		// packing into the array's leftover bits and aliasing an element.
		f.off.toWordBoundary()
		start := f.off.Byte
		for i := 0; i < count; i++ {
			f.off.allocBool()
		}
		f.off.closeBoolByte()
		f.appendResource(v, full, common.ValueTypeBoolArray, start,
			map[string]any{attrArraySize: count}, nil)
		return nil
	}

	scalar, ok := scalarTypes[elem]
	if !ok {
		// The element size is unknown, so the cursor cannot advance past the array:
		// every later address would be wrong.
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"'%s': unsupported array element type %q", full, elem), nil)
	}
	arrVT, ok := arrayTypes[scalar.valueType]
	if !ok {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"'%s': no XRT array value type for %q", full, scalar.valueType), nil)
	}
	start := f.off.alloc(scalar.byteSize*count, 2)
	f.appendResource(v, full, arrVT, start, map[string]any{attrArraySize: count}, nil)
	return nil
}

// A quoted type name is all that remains once every built-in table has missed.
func (f *flattener) handleUDTReference(v tiaVar, full string) errors.EdgeX {
	bare := strings.Trim(v.TIAType, `"`)
	udtKey := strings.ToUpper(bare)
	if _, ok := f.udts[udtKey]; ok {
		size, err := f.sizeOfUDT(udtKey, full)
		if err != nil {
			return err
		}
		// Support level is skip: advance past it, emit nothing. The parser keeps the
		// member list, so flattening them can be added without reworking the parse.
		f.off.alloc(size, 2)
		f.warnf("'%s' (%s): PLC data types are not converted – skipped (offset advanced by %d).",
			full, bare, size)
		return nil
	}

	// No TYPE block defined it. Say so rather than calling it an unknown built-in:
	// the fix is to re-export with dependent blocks, not to hunt for a typo.
	if strings.HasPrefix(v.TIAType, `"`) {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"'%s': PLC data type %s has no TYPE definition in this file; re-export "+
				"with 'Including all dependent blocks'", full, v.TIAType), nil)
	}

	// No known size means the cursor cannot advance, so every later resource would
	// sit at the wrong address — harder to notice than an outright failure.
	return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
		"'%s': unknown type %q", full, v.TIAType), nil)
}

// appendResource builds one resource and appends it, filling in what every caller
// shares: the sanitised name, the DB number and start, the declaration's comment
// and readWrite, and the DB resource type.
//
// extra carries the attributes that apply to some types only — bitIndex, size,
// array_size, counterType, and the IEC_Timer/IEC_Counter override of type — and
// may be nil. maximum is nil except for a type whose range is narrower than the
// integer carrying it (see scalarInfo.maximum).
func (f *flattener) appendResource(v tiaVar, full, valueType string, start int,
	extra map[string]any, maximum *float64) {
	attrs := map[string]any{
		attrType:     typeDB,
		attrDBNumber: f.dbNumber,
		attrStart:    start,
	}
	for k, val := range extra {
		attrs[k] = val
	}
	*f.resources = append(*f.resources, dtos.DeviceResource{
		Name:        sanitize(full),
		Description: v.Comment,
		Attributes:  attrs,
		Properties: dtos.ResourceProperties{
			ValueType: valueType,
			ReadWrite: v.ReadWrite,
			Maximum:   maximum,
		},
	})
}

func (f *flattener) warnf(format string, args ...any) {
	if f.quiet {
		return
	}
	f.lc.Warnf("tia: "+format, args...)
}

// sizeOfUDT measures a UDT by the same rules as an inline struct: each member
// word-aligned, the whole closing word-aligned. It recurses because a UDT may
// reference another.
func (f *flattener) sizeOfUDT(udtName, declaredAt string) (int, errors.EdgeX) {
	if size, done := f.sizes[udtName]; done {
		return size, nil
	}
	if f.resolving[udtName] {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf(
			"'%s': PLC data type %q references itself", declaredAt, udtName), nil)
	}
	f.resolving[udtName] = true
	defer delete(f.resolving, udtName)

	// resolving and sizes are shared, not fresh: the cycle guard has to span the
	// whole recursion, and a nested UDT measured here must count for later
	// references too. Everything else is scratch — only the total is kept.
	sub := &flattener{
		off:       &offsetTracker{},
		dbNumber:  f.dbNumber,
		udts:      f.udts,
		names:     map[string]string{},
		resolving: f.resolving,
		sizes:     f.sizes,
		resources: &[]dtos.DeviceResource{},
		lc:        f.lc,
		quiet:     true,
	}
	if err := sub.walkMembers(f.udts[udtName], ""); err != nil {
		return 0, err
	}
	sub.off.toWordBoundary()
	f.sizes[udtName] = sub.off.Byte
	return sub.off.Byte, nil
}

// ─── Helper functions ─────────────────────────────────────────────────────────

// matchSizedString matches String[n] / WString[n], returning the type constant
// and the bracketed length as written.
func matchSizedString(typeRaw string) (tiaType, lenText string, ok bool) {
	if m := reStringLen.FindStringSubmatch(typeRaw); m != nil {
		return typeString, m[1], true
	}
	if m := reWStringLen.FindStringSubmatch(typeRaw); m != nil {
		return typeWString, m[1], true
	}
	return "", "", false
}

// stripBlockComments replaces each (* *) comment with a space and its own
// newlines, so it neither welds the tokens it separated nor merges the lines it
// spanned. Line comments survive; they become resource descriptions.
func stripBlockComments(text string) string {
	return reBlockComment.ReplaceAllStringFunc(text, func(m string) string {
		return " " + strings.Repeat("\n", strings.Count(m, "\n"))
	})
}

// splitComment separates a line's code from its trailing // comment. A "//"
// inside a quoted name is part of the name, not a comment: TIA quoting exists so
// a name can hold characters an identifier cannot, and cutting there would drop
// the declaration and leave the offset cursor un-advanced.
func splitComment(line string) (code, comment string) {
	inQuotes := false
	for i := 0; i < len(line); i++ {
		switch {
		case line[i] == '"':
			inQuotes = !inQuotes
		case !inQuotes && line[i] == '/' && i+1 < len(line) && line[i+1] == '/':
			return strings.TrimRight(line[:i], " \t\r"), strings.TrimSpace(line[i+2:])
		}
	}
	return strings.TrimRight(line, " \t\r"), ""
}

func stripAttrBlocks(s string) string {
	return strings.TrimSpace(reAttrBlock.ReplaceAllString(s, ""))
}

// readWriteFor reports the access a declaration allows an external client (see
// reExternalWritableFalse).
func readWriteFor(rawLine string) string {
	if reExternalWritableFalse.MatchString(rawLine) {
		return common.ReadWrite_R
	}
	return common.ReadWrite_RW
}

// sanitize removes double-quotes and replaces characters invalid in XRT
// resource names with underscores.
func sanitize(name string) string {
	s := reSanitizeQuote.ReplaceAllString(name, "")
	return reSanitizeInvalid.ReplaceAllString(s, "_")
}

// scalarMaximum converts scalarInfo.maximum's zero-means-none into a nil pointer.
func scalarMaximum(info scalarInfo) *float64 {
	if info.maximum == 0 {
		return nil
	}
	bound := info.maximum
	return &bound
}

// stringLenOrDefault substitutes the S7 default where no length was declared (see
// noStringLen).
func stringLenOrDefault(declared int) int {
	if declared == noStringLen {
		return defaultStringLen
	}
	return declared
}

func parseArraySig(typeStr string) (lo, hi int, elemType string, declaredStringLen int, ok bool) {
	m := reArraySig.FindStringSubmatch(strings.TrimSpace(typeStr))
	if m == nil {
		return 0, 0, "", 0, false
	}
	// Atoi returns the clamped MaxInt64/MinInt64 alongside its error, so the
	// error must be checked: ignoring it would feed that clamped value into the
	// count and offset arithmetic.
	var err error
	if lo, err = strconv.Atoi(m[1]); err != nil {
		return 0, 0, "", 0, false
	}
	if hi, err = strconv.Atoi(m[2]); err != nil {
		return 0, 0, "", 0, false
	}
	elemRaw := strings.TrimSpace(m[3])
	if sm := reStringLen.FindStringSubmatch(elemRaw); sm != nil {
		slen, serr := strconv.Atoi(sm[1])
		if serr != nil || slen > maxStringLen {
			return 0, 0, "", 0, false
		}
		return lo, hi, typeString, slen, true
	}
	if strings.EqualFold(elemRaw, typeString) {
		return lo, hi, typeString, noStringLen, true
	}
	if sm := reWStringLen.FindStringSubmatch(elemRaw); sm != nil {
		slen, serr := strconv.Atoi(sm[1])
		if serr != nil || slen > maxStringLen {
			return 0, 0, "", 0, false
		}
		return lo, hi, typeWString, slen, true
	}
	if strings.EqualFold(elemRaw, typeWString) {
		return lo, hi, typeWString, noStringLen, true
	}
	return lo, hi, normaliseType(strings.ToUpper(elemRaw)), noStringLen, true
}
