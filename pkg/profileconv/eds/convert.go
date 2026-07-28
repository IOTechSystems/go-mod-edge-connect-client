// Copyright (C) 2026 IOTech Ltd

// Package eds converts an EtherNet/IP EDS (Electronic Data Sheet) into an EdgeX
// DeviceProfile consumed by the XRT EtherNet/IP device service. Its Convert
// function satisfies profileconv.ConvertFunc, so it slots in alongside other
// source formats (xlsx, DBC…).
//
// The conversion is a three-stage pipeline, one file per stage, so each layer
// owns a distinct concern and is tested in isolation:
//
//	parse    (parser.go)     EDS text  -> *eds: a generic, semantics-free tree
//	                         of sections/entries/fields. Handles ONLY EDS grammar
//	                         (";" termination, "," fields, "$" comments, quoting,
//	                         multi-line statements) — no CIP meaning.
//	extract  (extractor.go)  *eds -> *extracted: applies CIP meaning to the tree,
//	                         pulling the positional fields the mapper needs into
//	                         named structs (params, assemblies, connections, enum
//	                         maps). Field-index knowledge lives here and nowhere
//	                         else; the three mappers all read this one struct.
//	map      (mapper*.go)    *extracted -> dtos.DeviceProfile: builds the device
//	                         resources (implicit I/O, settings, explicit messaging)
//	                         and enum DeviceCommands.
//
// Splitting extract from map keeps "which field index means what" in one place
// and lets each mapper consume named fields instead of re-reading the raw tree.
// This file (convert.go) is the entry point that orchestrates the three stages.
package eds

import (
	"bytes"
	"context"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv"
)

// compile-time check that Convert satisfies the format-neutral contract.
var _ profileconv.ConvertFunc = Convert

// Convert turns one EtherNet/IP EDS file into an EdgeX DeviceProfile.
//
//	var convert profileconv.ConvertFunc = eds.Convert
//	profile, err := convert(ctx, lc, data, nil)
//
// ctx and options are part of the ConvertFunc contract but currently unused:
// parsing is a synchronous in-memory pass with no cancellation point, and
// options is reserved for format-specific tuning.
func Convert(ctx context.Context, lc logger.LoggingClient,
	data []byte, options map[string]any) (dtos.DeviceProfile, errors.EdgeX) {
	var profile dtos.DeviceProfile

	// Stage 1 — parse: EDS text into a semantics-free section/entry/field tree.
	edsTree, err := parse(bytes.NewReader(data))
	if err != nil {
		return profile, errors.NewCommonEdgeXWrapper(err)
	}

	// Stage 2 — extract: apply CIP meaning, producing the structured middle layer.
	extractedEds, err := extract(lc, edsTree)
	if err != nil {
		return profile, errors.NewCommonEdgeXWrapper(err)
	}

	// Stage 3 — map: build the EdgeX device resources and commands.
	profile, err = extractedEds.mapToProfile()
	if err != nil {
		return profile, errors.NewCommonEdgeXWrapper(err)
	}

	// A named profile with no resources is not convertible content: a caller
	// could not tell it apart from a successful conversion. Reject it explicitly
	// (e.g. a modular device whose dynamic assemblies were all skipped).
	if len(profile.DeviceResources) == 0 {
		return profile, errors.NewCommonEdgeX(errors.KindContractInvalid, "EDS produced no device resources", nil)
	}

	// Validate the full struct (required fields, ValueType/ReadWrite enums) as
	// well as the DTO-level duplicate-name/binary-write rules; ValidateDeviceProfileDTO
	// alone would skip the struct-tag checks.
	if verr := profile.Validate(); verr != nil {
		return profile, errors.NewCommonEdgeXWrapper(verr)
	}
	return profile, nil
}
