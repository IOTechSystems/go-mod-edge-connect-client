// Copyright (C) 2026 IOTech Ltd

// This file is the EDS mapping layer: it turns the extracted middle layer into
// an EdgeX DeviceProfile, applying the CIP-to-profile rules.

package eds

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// XRT EtherNet/IP resource attribute keys and type values.
const (
	maxOffsetBytes = 2000 // XRT implicit I/O offsetBytes upper bound

	// A param name with this prefix marks a pad/reserved member (EDS convention,
	// e.g. "RESERVED_pad5"); see isPadMember.
	reservedPrefix = "RESERVED"
)

// resourceBuilder builds one kind of device resource, appending unique names to
// seen. Implicit I/O, settings and explicit messaging each provide one (as a
// method on *extracted).
type resourceBuilder func(names *nameSet) ([]dtos.DeviceResource, errors.EdgeX)

// mapToProfile builds the EdgeX DeviceProfile from the extracted middle layer:
// the identity shell plus device resources (implicit I/O, settings, explicit
// messaging).
func (x *extracted) mapToProfile() (dtos.DeviceProfile, errors.EdgeX) {
	var profile dtos.DeviceProfile

	x.enumOps = nil // per-pass scratch; clear so remapping stays idempotent

	// name (a sanitized identifier) and model (the human-readable original) both
	// come from ProdName, falling back to Catalog. Fall back on the sanitized
	// result being empty, not the raw string, so a ProdName of only whitespace
	// (which sanitizes to "") still yields to a usable Catalog.
	model := x.device.productName
	name := sanitizeName(model)
	if name == "" {
		model = x.device.catalog
		name = sanitizeName(model)
	}
	if name == "" {
		return profile, errors.NewCommonEdgeX(errors.KindContractInvalid, "cannot derive profile name: EDS [Device] has no usable ProdName or Catalog", nil)
	}

	profile.Name = name
	profile.Manufacturer = x.device.vendorName
	profile.Model = model

	// A single name set shared across all resource kinds keeps every name unique
	// for ValidateDeviceProfileDTO.
	names := newNameSet()
	var resources []dtos.DeviceResource
	for _, build := range []resourceBuilder{x.mapImplicitIO, x.mapSettings, x.mapExplicit} {
		built, err := build(names)
		if err != nil {
			return profile, err
		}
		resources = append(resources, built...)
	}
	profile.DeviceResources = resources
	profile.DeviceCommands = x.enumCommands()
	return profile, nil
}

// enumCommands turns the enum resources collected during I/O mapping into
// DeviceCommands, each with a single resourceOperation carrying the value->label
// mappings. Returns nil when there are none, so the profile omits deviceCommands.
func (x *extracted) enumCommands() []dtos.DeviceCommand {
	if len(x.enumOps) == 0 {
		return nil
	}
	commands := make([]dtos.DeviceCommand, 0, len(x.enumOps))
	for _, op := range x.enumOps {
		commands = append(commands, dtos.DeviceCommand{
			Name:      op.resource,
			ReadWrite: op.readWrite,
			ResourceOperations: []dtos.ResourceOperation{
				{DeviceResource: op.resource, Mappings: op.mappings},
			},
		})
	}
	return commands
}

// mapImplicitIO builds the implicit I/O resources from the assemblies a
// connection uses. Direction comes from the connection: an assembly referenced
// as O2T format is output (W), as T2O format is input (R).
//
// Per the XRT model a bidirectional point is two distinct resources — one O2T
// and one T2O, each with its own offset — not one RW resource, so a param used
// in both an output and an input assembly produces two resources. Resource names
// must be unique for ValidateDeviceProfileDTO, so a colliding name is suffixed
// (see nameSet).
//
// Each assembly's members are walked in order, accumulating a bit offset; a pad
// member advances the offset but yields no resource. Alignment padding is
// expected to be expressed explicitly by the EDS (a pad member filling the gap),
// so offsets are a plain running sum of member bit lengths, starting at bit 0.
func (x *extracted) mapImplicitIO(names *nameSet) ([]dtos.DeviceResource, errors.EdgeX) {
	var resources []dtos.DeviceResource
	expanded := map[string]bool{} // (assembly, direction) pairs already built

	for _, c := range x.connections {
		for _, ref := range []struct {
			asmName string
			typ     string
		}{
			{c.o2tFormat, typeO2T},
			{c.t2oFormat, typeT2O},
		} {
			if ref.asmName == "" {
				continue
			}
			// Multiple connections often share one assembly (e.g. exclusive-owner
			// plus listen-only); build each (assembly, direction) only once.
			key := ref.asmName + "/" + ref.typ
			if expanded[key] {
				continue
			}
			expanded[key] = true

			built, err := x.buildAssemblyResources(ref.asmName, ref.typ, names)
			if err != nil {
				return nil, err
			}
			resources = append(resources, built...)
		}
	}
	return resources, nil
}

// buildAssemblyResources walks one assembly's members in order, accumulating a
// bit offset, and returns one I/O resource per non-pad member. typ is the XRT
// direction type (O2T = write, T2O = read). seen carries the resource names
// already emitted so uniqueName can keep every name distinct.
func (x *extracted) buildAssemblyResources(asmName, typ string, names *nameSet) ([]dtos.DeviceResource, errors.EdgeX) {
	asm, err := x.assemblyByName(asmName)
	if err != nil {
		return nil, err
	}

	readWrite := common.ReadWrite_W
	if typ == typeT2O {
		readWrite = common.ReadWrite_R
	}

	var resources []dtos.DeviceResource
	offsetBits := 0
	for _, m := range asm.members {
		r, bits, produced, err := x.buildMemberResource(asmName, typ, readWrite, m, offsetBits, names)
		if err != nil {
			return nil, err
		}
		if produced {
			resources = append(resources, r)
		}
		offsetBits += bits
	}

	if err := checkAssemblySize(asmName, asm.size, offsetBits); err != nil {
		return nil, err
	}
	return resources, nil
}

// buildMemberResource maps one assembly member at the running offsetBits. A pad
// member advances the offset but yields no resource (produced=false); a real
// member returns its resource. bits is always the member's bit length so the
// caller can advance the offset regardless.
func (x *extracted) buildMemberResource(asmName, typ, readWrite string, m assemblyMember, offsetBits int, names *nameSet) (r dtos.DeviceResource, bits int, produced bool, err errors.EdgeX) {
	p, hasParam := x.params[m.paramRef]

	bits, err = memberBitLength(m, p, hasParam)
	if err != nil {
		return r, 0, false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q member %q", asmName, m.paramRef), err)
	}
	if isPadMember(m, p) {
		return r, bits, false, nil
	}
	if !hasParam {
		return r, 0, false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q member references unknown param %q", asmName, m.paramRef), nil)
	}

	valueType, verr := valueTypeForCIP(p.dataType)
	if verr != nil {
		return r, 0, false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q member %q", asmName, m.paramRef), verr)
	}
	offsetBytes := offsetBits / 8
	if offsetBytes > maxOffsetBytes {
		return r, 0, false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q member %q offsetBytes %d exceeds max %d", asmName, m.paramRef, offsetBytes, maxOffsetBytes), nil)
	}
	attrs := map[string]any{
		attrType:        typ,
		attrOffsetBytes: offsetBytes,
		attrOffsetBits:  offsetBits % 8,
	}
	// bitLength is optional for Bool (a single bit); omit it to match XRT
	// profiles, which leave it off Bool resources.
	if valueType != common.ValueTypeBool {
		attrs[attrBitLength] = bits
	}
	resName := names.unique(p.name, typ)
	// An enumerated param becomes a DeviceCommand with resourceOperation.mappings;
	// record it here so the command targets the emitted (possibly suffixed) name.
	if len(p.enum) > 0 {
		x.enumOps = append(x.enumOps, enumOp{resource: resName, readWrite: readWrite, mappings: p.enum})
	}
	return dtos.DeviceResource{
		Name:        resName,
		Description: p.help,
		Attributes:  attrs,
		Properties:  paramProperties(p, valueType, readWrite),
	}, bits, true, nil
}

// checkAssemblySize cross-checks members against the assembly's declared Size:
// the members must not overrun the declared byte length. Catches an EDS whose
// Size and member layout disagree, which would otherwise produce an inconsistent
// profile. A blank or unparseable Size is not enforced.
func checkAssemblySize(asmName, size string, offsetBits int) errors.EdgeX {
	sz := strings.TrimSpace(size)
	if sz == "" {
		return nil
	}
	declared, err := strconv.Atoi(sz)
	if err != nil || declared < 0 {
		return nil
	}
	if used := (offsetBits + 7) / 8; used > declared {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q members use %d bytes but its declared size is %d", asmName, used, declared), nil)
	}
	return nil
}

// paramProperties builds the ResourceProperties for a param: its value type and
// readWrite plus the optional engineering fields (units, scale, base, offset,
// min/max, default) mapped from the EDS [Params] entry. scale is
// Mult ÷ Div. Fields absent or unparseable in the EDS are left unset.
func paramProperties(p param, valueType, readWrite string) dtos.ResourceProperties {
	props := dtos.ResourceProperties{
		ValueType:    valueType,
		ReadWrite:    readWrite,
		Units:        p.units,
		DefaultValue: strings.TrimSpace(p.defaultVal),
		Minimum:      parseFloat(p.minimum),
		Maximum:      parseFloat(p.maximum),
		Base:         parseFloat(p.scaleBase),
		Offset:       parseFloat(p.scaleOff),
		Scale:        scaleFactor(p.scaleMult, p.scaleDiv),
	}
	return props
}

// parseFloat parses an EDS numeric field into a *float64, or nil if blank or
// unparseable (the field is optional; a bad value is simply not carried).
func parseFloat(s string) *float64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &f
}

// scaleFactor computes the scale as Mult ÷ Div, or nil unless both are present
// and Div is non-zero.
func scaleFactor(mult, div string) *float64 {
	m, d := parseFloat(mult), parseFloat(div)
	if m == nil || d == nil || *d == 0 {
		return nil
	}
	s := *m / *d
	return &s
}

// nameSet allocates unique device-resource names. used records every emitted
// name (resource names must be unique for ValidateDeviceProfileDTO); nextN
// remembers the next numeric suffix per base so allocation stays O(1) amortised
// rather than re-scanning on every collision.
type nameSet struct {
	used  map[string]bool
	nextN map[string]int
}

func newNameSet() *nameSet {
	return &nameSet{used: map[string]bool{}, nextN: map[string]int{}}
}

// unique returns a name not yet emitted, and records it. It tries base, then
// base+"_"+typ (the common case: the same point in both directions, e.g.
// Setpoint / Setpoint_T2O), then base_2, base_3… as a last resort.
func (n *nameSet) unique(base, typ string) string {
	if !n.used[base] {
		n.used[base] = true
		return base
	}
	if c := base + "_" + typ; !n.used[c] {
		n.used[c] = true
		return c
	}
	i := n.nextN[base]
	if i < 2 {
		i = 2
	}
	for {
		c := base + "_" + strconv.Itoa(i)
		i++
		if !n.used[c] {
			n.used[c] = true
			n.nextN[base] = i
			return c
		}
	}
}

// isPadMember reports whether a member is padding: it has no param reference, or
// the referenced param is named with the RESERVED prefix (EDS pad convention).
// Pads occupy offset but produce no resource. p is the member's param (zero
// value if the ref is unknown, in which case only the empty-ref case matters).
func isPadMember(m assemblyMember, p param) bool {
	if m.paramRef == "" {
		return true
	}
	return strings.HasPrefix(p.name, reservedPrefix)
}

// memberBitLength returns a member's bit length: the assembly's explicit member
// size when present, else the referenced param's Data Size (bytes) in bits. The
// length must be positive. p/hasParam are the member's looked-up param.
func memberBitLength(m assemblyMember, p param, hasParam bool) (int, errors.EdgeX) {
	if m.bitLength != "" {
		n, err := strconv.Atoi(strings.TrimSpace(m.bitLength))
		if err != nil {
			return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("invalid bit length %q", m.bitLength), err)
		}
		return checkBits(n)
	}
	if !hasParam {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, "no bit length and no param to take a Data Size from", nil)
	}
	sizeBytes := strings.TrimSpace(p.dataSize)
	if sizeBytes == "" {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, "no bit length and param has no Data Size", nil)
	}
	n, err := strconv.Atoi(sizeBytes)
	if err != nil {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("invalid param Data Size %q", sizeBytes), err)
	}
	// Bound the byte count before multiplying so n*8 cannot overflow.
	if n < 0 || n > maxOffsetBytes {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("param Data Size %d out of range 0-%d", n, maxOffsetBytes), nil)
	}
	return checkBits(n * 8)
}

// maxBitLength bounds a single member's bit length (the whole implicit I/O area
// is at most maxOffsetBytes). It also keeps the running offset from overflowing.
const maxBitLength = maxOffsetBytes * 8

// checkBits rejects a bit length outside 1..maxBitLength. Zero/negative would
// give a zero-length resource or a negative offset; an over-large value would
// overflow the running offset.
func checkBits(n int) (int, errors.EdgeX) {
	if n <= 0 || n > maxBitLength {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("bit length %d out of range 1-%d", n, maxBitLength), nil)
	}
	return n, nil
}

// sanitizeName turns a product name into a lower-cased identifier, e.g.
// "EtherNetIP Sample" -> "ethernetip-sample". EdgeX only requires a non-empty
// name, but a clean identifier avoids surprises where the name is used as a key.
// Non-ASCII letters are kept (lower-cased) by design — EdgeX does not restrict
// the character set.
func sanitizeName(s string) string {
	// Spaces, tabs, slashes and existing hyphens are all separators; collapse
	// any run of them to a single hyphen, then trim leading/trailing hyphens.
	isSeparator := func(r rune) bool {
		return r == ' ' || r == '\t' || r == '/' || r == '\\' || r == '-'
	}
	var b strings.Builder
	prevHyphen := false
	for _, r := range strings.ToLower(s) {
		switch {
		case isSeparator(r):
			if !prevHyphen && b.Len() > 0 {
				b.WriteByte('-')
				prevHyphen = true
			}
		default:
			b.WriteRune(r)
			prevHyphen = false
		}
	}
	return strings.Trim(b.String(), "-")
}
