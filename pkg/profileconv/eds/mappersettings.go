// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

const (
	// XRT Settings value bounds.
	maxAssemblyID = 65535
	maxIOSize     = 2000 // O2T / T2O assembly size (bytes)
	maxConfigSize = 400  // Config assembly size (bytes)

	// A Settings resource carries no data value, but EdgeX requires valueType and
	// readWrite; these placeholders match what XRT profiles use.
	settingsValueType = common.ValueTypeString
	settingsReadWrite = common.ReadWrite_R

	// Header-format field values in the connection-parameters DWORD:
	// 4 = 4-byte run/idle header.
	headerRunIdle        = 4
	o2tHeaderFormatShift = 8  // bits 8-10
	t2oHeaderFormatShift = 12 // bits 12-14
	headerFormatMask     = 0x7
)

// mapSettings builds one Settings resource per assembly a connection
// uses: O2TSettings / T2OSettings / ConfigSettings. This is the "1 Settings + N
// I/O" fan-out. Each (assembly, settings-type) pair is emitted once even if
// several connections share the assembly.
func (x *extracted) mapSettings(names *nameSet) ([]dtos.DeviceResource, errors.EdgeX) {
	var resources []dtos.DeviceResource
	// header32 of each (assembly, settings-type) already emitted, so a shared
	// assembly is emitted once and a conflicting header across connections is
	// caught rather than silently taking the first.
	done := map[string]bool{}

	for _, c := range x.connections {
		for _, s := range settingsRefs(c) {
			if s.asmName == "" {
				continue
			}
			skip, err := seenSettings(done, s)
			if err != nil {
				return nil, err
			}
			if skip {
				continue
			}

			r, err := x.buildSettingsResource(s, names)
			if err != nil {
				return nil, err
			}
			resources = append(resources, r)
		}
	}
	return resources, nil
}

// seenSettings records the (assembly, settings-type) target in done and reports
// whether it was already emitted (skip=true). A shared assembly whose header32
// differs across connections is a conflict error, not a silent first-wins.
func seenSettings(done map[string]bool, s settingsRef) (skip bool, err errors.EdgeX) {
	key := s.asmName + "/" + s.settType
	if prev, ok := done[key]; ok {
		if prev != s.header32 {
			return false, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q is used by connections with conflicting %s", s.asmName, attrIncludeHeader32bit), nil)
		}
		return true, nil
	}
	done[key] = s.header32
	return false, nil
}

// settingsRef is one (assembly, settings-type) target derived from a connection.
type settingsRef struct {
	asmName  string
	settType string
	header32 bool
}

// settingsRefs expands a connection into its O2T/T2O/Config settings targets.
func settingsRefs(c connection) []settingsRef {
	o2tHdr, t2oHdr := connectionHeader32bit(c.connectionParams)
	return []settingsRef{
		{c.o2tFormat, typeO2TSettings, o2tHdr},
		{c.t2oFormat, typeT2OSettings, t2oHdr},
		{asmName: c.configFormat, settType: typeConfigSettings}, // config has no header
	}
}

// buildSettingsResource builds the Settings resource for one target: it resolves
// the assembly, bounds-checks its id/size, and assembles the attributes.
func (x *extracted) buildSettingsResource(s settingsRef, names *nameSet) (dtos.DeviceResource, errors.EdgeX) {
	asm, aerr := x.assemblyByName(s.asmName)
	if aerr != nil {
		return dtos.DeviceResource{}, aerr
	}
	if asm.assemblyID < 0 || asm.assemblyID > maxAssemblyID {
		return dtos.DeviceResource{}, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly %q assemblyID %d out of range 0-%d", s.asmName, asm.assemblyID, maxAssemblyID), nil)
	}
	sizeMax := maxIOSize
	if s.settType == typeConfigSettings {
		sizeMax = maxConfigSize
	}
	size, err := parseAssemblySize(asm, sizeMax)
	if err != nil {
		return dtos.DeviceResource{}, err
	}

	attrs := map[string]any{
		attrType:       s.settType,
		attrAssemblyID: asm.assemblyID,
		attrSize:       size,
	}
	if s.settType != typeConfigSettings { // only O2T/T2O carry the header flag
		attrs[attrIncludeHeader32bit] = s.header32
	}
	return dtos.DeviceResource{
		Name:        names.unique(settingsName(asm, s.settType), s.settType),
		Description: asm.name,
		Attributes:  attrs,
		Properties: dtos.ResourceProperties{
			ValueType: settingsValueType,
			ReadWrite: settingsReadWrite,
		},
	}, nil
}

// connectionHeader32bit decodes the O2T and T2O includeHeader32bit flags from the
// connection-parameters DWORD (field 1). The O2T header format is bits 8-10 and
// the T2O header format bits 12-14; a value of 4 means a 4-byte run/idle header.
// A blank or unparseable field defaults to false.
func connectionHeader32bit(params string) (o2t, t2o bool) {
	s := strings.TrimSpace(params)
	if s == "" {
		return false, false
	}
	v, err := strconv.ParseUint(s, 0, 32)
	if err != nil {
		return false, false
	}
	o2t = (v>>o2tHeaderFormatShift)&headerFormatMask == headerRunIdle
	t2o = (v>>t2oHeaderFormatShift)&headerFormatMask == headerRunIdle
	return o2t, t2o
}

// settingsName derives a readable Settings resource name from the assembly name,
// falling back to "Assembly<id> <type>" when the assembly is unnamed so the name
// still traces back to a specific assembly.
func settingsName(asm assembly, settType string) string {
	if asm.name != "" {
		return asm.name + " " + settType
	}
	return fmt.Sprintf("Assembly%d %s", asm.assemblyID, settType)
}

// parseAssemblySize parses an assembly's declared Size (bytes). An empty size is
// 0 (some assemblies declare no fixed size); a negative, non-numeric, or over-max
// size is an error. max is the settings-type-specific upper bound.
func parseAssemblySize(asm assembly, max int) (int, errors.EdgeX) {
	s := strings.TrimSpace(asm.size)
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("invalid assembly size %q", asm.size), err)
	}
	if n < 0 || n > max {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("assembly size %d out of range 0-%d", n, max), nil)
	}
	return n, nil
}
