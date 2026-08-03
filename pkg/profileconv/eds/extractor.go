// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"fmt"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// extracted is the structured, still-CIP-agnostic middle layer between the raw
// AST and the mapper. It pulls the fields the mapper needs out of positional
// entries so later stages read named fields, not slice indices. Field positions
// follow the ODVA EDS spec.
type extracted struct {
	device      deviceInfo
	params      map[string]param // keyed by ParamN name, e.g. "Param1"
	assemblies  map[string]assembly
	connections []connection
	// isModular is set when a modular-device marker is seen while walking the
	// EDS: a [Modular] section or a ProxyConnectN entry. Such a device's dynamic
	// I/O cannot be derived from a single EDS; how to surface that is not
	// decided yet, so this flag is detected but not yet acted on.
	isModular bool
	// enumOps accumulates the (resource, readWrite, mappings) triples for I/O
	// resources whose param carries an EnumN, collected while building resources
	// (so names match the emitted resource) and turned into DeviceCommands.
	enumOps []enumOp
}

// enumOp records an emitted I/O resource that has an enum value->label map, used
// to build a DeviceCommand carrying resourceOperation.mappings.
type enumOp struct {
	resource  string
	readWrite string
	mappings  map[string]string
}

// deviceInfo holds the [Device] identity fields used to build the profile shell.
type deviceInfo struct {
	vendorName  string
	productName string
	catalog     string
}

// param is one [Params] ParamN entry, fields kept verbatim; the mapper casts
// them per the data type. linkPath non-empty marks an explicit-messaging param.
type param struct {
	name       string
	dataType   string // CIP type code as written (e.g. "0xC3")
	dataSize   string
	units      string
	help       string
	minimum    string
	maximum    string
	defaultVal string
	scaleMult  string
	scaleDiv   string
	scaleBase  string
	scaleOff   string
	linkPath   string
	enum       map[string]string // enum value -> label, from the matching EnumN entry
}

// assembly is one [Assembly] AssemN: its parsed instance id, declared size, and
// ordered members. Each member is a (bit length, ParamN reference) pair; the
// order determines each point's offset (computed later, in the mapper).
type assembly struct {
	name       string
	assemblyID int
	size       string
	members    []assemblyMember
}

type assemblyMember struct {
	bitLength string
	paramRef  string // ParamN name, or "" for an unreferenced/pad slot
}

// connection is one [Connection Manager] ConnectionN, reduced to the references
// that decide direction and the header flag. o2tFormat/t2oFormat/configFormat
// name the assemblies this connection uses ("" if absent).
type connection struct {
	connectionParams string // field 1: real-time format bit -> includeHeader32bit
	o2tFormat        string // field 4: output assembly (writable)
	t2oFormat        string // field 7: input assembly (read-only)
	configFormat     string // field 11: config assembly
}

// Entry keyword prefixes that mark the numbered entries in each section
// (ParamN, EnumN, AssemN, ConnectionN, ProxyConnectN). Other keywords (object
// metadata) are skipped.
const (
	keywordParam        = "Param"
	keywordEnum         = "Enum"
	keywordAssembly     = "Assem"
	keywordConnection   = "Connection"
	keywordProxyConnect = "ProxyConnect"
)

// [Device] identity keywords used to build the profile shell.
const (
	deviceVendName = "VendName"
	deviceProdName = "ProdName"
	deviceCatalog  = "Catalog"
)

// Field indices into each positional entry (0-indexed on the fields after "=").
// Positions follow the ODVA EDS spec for [Params], [Assembly], and [Connection].

// [Params] ParamN fields.
const (
	paramLinkPath  = 2
	paramDataType  = 4
	paramDataSize  = 5
	paramName      = 6
	paramUnits     = 7
	paramHelp      = 8
	paramMinimum   = 9
	paramMaximum   = 10
	paramDefault   = 11
	paramScaleMult = 12
	paramScaleDiv  = 13
	paramScaleBase = 14
	paramScaleOff  = 15
)

// [Assembly] AssemN fields; members are (size, ref) pairs from assemblyFirstMember.
const (
	assemblyPath        = 1
	assemblySize        = 2
	assemblyFirstMember = 6
)

// [Connection Manager] ConnectionN fields.
const (
	connectionParams = 1
	connectionO2T    = 4
	connectionT2O    = 7
	connectionConfig = 11
)

// extract reduces the parsed AST to the structured middle layer. Missing
// sections yield empty maps rather than errors — the mapper decides what is
// required — but a malformed assembly Path (unparseable instance id) is an error.
func extract(lc logger.LoggingClient, e *eds) (*extracted, errors.EdgeX) {
	out := &extracted{
		params:     map[string]param{},
		assemblies: map[string]assembly{},
	}

	if dev := e.section(sectionDevice); dev != nil {
		out.device = deviceInfo{
			vendorName:  entryField(dev, deviceVendName),
			productName: entryField(dev, deviceProdName),
			catalog:     entryField(dev, deviceCatalog),
		}
	}

	extractParams(out, e)
	if err := extractAssemblies(lc, out, e); err != nil {
		return nil, err
	}
	// A [Modular] section marks a modular device.
	if e.section(sectionModular) != nil {
		out.isModular = true
	}
	extractConnections(out, e)

	return out, nil
}

// extractParams fills out.params from the [Params] section. A ParamN's matching
// EnumN sub-entry (paired by N) is attached as its value->label map.
func extractParams(out *extracted, e *eds) {
	for _, en := range e.sectionEntries(sectionParams) {
		// EnumN must be checked first: it is not a Param prefix, but keep the
		// dispatch explicit so a future keyword is not silently misrouted.
		if strings.HasPrefix(en.keyword, keywordEnum) {
			continue // handled in the merge pass below
		}
		if !strings.HasPrefix(en.keyword, keywordParam) {
			continue // object metadata (Object_Name, Revision…)
		}
		out.params[en.keyword] = param{
			name:       en.field(paramName),
			dataType:   en.field(paramDataType),
			dataSize:   en.field(paramDataSize),
			units:      en.field(paramUnits),
			help:       en.field(paramHelp),
			minimum:    en.field(paramMinimum),
			maximum:    en.field(paramMaximum),
			defaultVal: en.field(paramDefault),
			scaleMult:  en.field(paramScaleMult),
			scaleDiv:   en.field(paramScaleDiv),
			scaleBase:  en.field(paramScaleBase),
			scaleOff:   en.field(paramScaleOff),
			linkPath:   en.field(paramLinkPath),
		}
	}
	attachEnums(out, e)
}

// attachEnums walks the [Params] section for EnumN entries and attaches each to
// its ParamN (paired by the number N). An EnumN whose ParamN is absent is
// ignored. EnumN fields are (value, label) pairs from index 0.
func attachEnums(out *extracted, e *eds) {
	for _, en := range e.sectionEntries(sectionParams) {
		if !strings.HasPrefix(en.keyword, keywordEnum) {
			continue
		}
		n := strings.TrimPrefix(en.keyword, keywordEnum)
		p, ok := out.params[keywordParam+n]
		if !ok {
			continue // Enum without a matching Param — nothing to attach to
		}
		mappings := map[string]string{}
		for i := 0; i+1 < len(en.fields); i += 2 {
			value := strings.TrimSpace(en.field(i))
			label := strings.TrimSpace(en.field(i + 1))
			if value != "" {
				mappings[value] = label
			}
		}
		if len(mappings) > 0 {
			p.enum = mappings
			out.params[keywordParam+n] = p
		}
	}
}

// extractAssemblies fills out.assemblies from the [Assembly] section. A blank
// Path (dynamic/placeholder assembly) is skipped; a malformed non-blank Path is
// an error.
func extractAssemblies(lc logger.LoggingClient, out *extracted, e *eds) errors.EdgeX {
	for _, en := range e.sectionEntries(sectionAssembly) {
		if !strings.HasPrefix(en.keyword, keywordAssembly) {
			continue // object metadata entries (Object_Name, Revision…)
		}
		// A blank Path is a dynamic/placeholder assembly (common in modular
		// devices, where the layout is not in the EDS). Skip it rather than
		// error; if a real connection references it, that surfaces later as an
		// unknown-assembly error.
		if strings.TrimSpace(strings.Trim(en.field(assemblyPath), `"`)) == "" {
			lc.Debugf("eds: skipping assembly %q with no Path (dynamic/placeholder assembly)", en.keyword)
			continue
		}
		id, err := assemblyIDFromPath(en.field(assemblyPath))
		if err != nil {
			return errors.NewCommonEdgeXWrapper(err)
		}
		out.assemblies[en.keyword] = assembly{
			name:       en.field(0),
			assemblyID: id,
			size:       en.field(assemblySize),
			members:    extractMembers(en),
		}
	}
	return nil
}

// extractConnections fills out.connections from the [Connection Manager] section
// and flags modular devices via ProxyConnectN entries.
func extractConnections(out *extracted, e *eds) {
	for _, en := range e.sectionEntries(sectionConnectionManager) {
		// ProxyConnectN entries are modular markers: connections proxied on
		// behalf of plug-in modules, whose dynamic I/O we cannot lay out.
		if strings.HasPrefix(en.keyword, keywordProxyConnect) {
			out.isModular = true
			continue
		}
		if !strings.HasPrefix(en.keyword, keywordConnection) {
			continue
		}
		out.connections = append(out.connections, connection{
			connectionParams: en.field(connectionParams),
			o2tFormat:        en.field(connectionO2T),
			t2oFormat:        en.field(connectionT2O),
			configFormat:     en.field(connectionConfig),
		})
	}
}

// assemblyByName returns the extracted assembly a connection references, or a
// KindContractInvalid error naming the missing assembly. Shared by the I/O and
// settings mappers so the "unknown assembly" error is worded in one place.
func (x *extracted) assemblyByName(name string) (assembly, errors.EdgeX) {
	asm, ok := x.assemblies[name]
	if !ok {
		return assembly{}, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("connection references unknown assembly %q", name), nil)
	}
	return asm, nil
}

// entryField returns field 0 of the named keyword entry in a section, or "".
// Used for the keyword=value entries of [Device].
func entryField(s *section, keyword string) string {
	if en := s.entry(keyword); en != nil {
		return en.field(0)
	}
	return ""
}

// extractMembers reads the (bit length, ParamN) member pairs that start at
// assemblyFirstMember. field(i+1) is "" when the ref column is the last field
// (an empty-size member ending the entry), so the pair is still captured rather
// than dropped.
func extractMembers(en *entry) []assemblyMember {
	var members []assemblyMember
	for i := assemblyFirstMember; i < len(en.fields); i += 2 {
		bits, ref := en.field(i), en.field(i+1)
		if bits == "" && ref == "" {
			continue // empty reserved slot between fields, not a member
		}
		members = append(members, assemblyMember{bitLength: bits, paramRef: ref})
	}
	return members
}

// assemblyIDFromPath parses the assembly instance id from an EDS Path EPATH via
// parseEPATH: the value of the first instance or connection-point segment.
// (Config paths legitimately carry several connection-point segments, e.g.
// "20 04 24 80 2C 70 2C 64"; the first is the assembly instance.)
func assemblyIDFromPath(path string) (int, errors.EdgeX) {
	segs, err := parseEPATH(path)
	if err != nil {
		return 0, err
	}
	for _, s := range segs {
		if s.logicalType == segInstance || s.logicalType == segConnectionPt {
			return s.value, nil
		}
	}
	return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("no assembly instance segment in path %q", path), nil)
}
