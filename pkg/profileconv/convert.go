// Copyright (C) 2026 IOTech Ltd

// Package profileconv defines the common contract for converting a vendor
// source file (EDS, xlsx, DBC, vendor JSON…) into an EdgeX DeviceProfile.
//
// This package holds only the format-neutral ConvertFunc type. Each concrete
// format lives in its own sub-package (e.g. profileconv/eds) and provides a
// Convert function that satisfies ConvertFunc — so a caller can swap formats
// without depending on any single format's package:
//
//	var convert profileconv.ConvertFunc = eds.Convert
//	profile, err := convert(ctx, lc, data, nil)
package profileconv

import (
	"context"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// ConvertFunc converts one source format's bytes into an EdgeX DeviceProfile.
//
//   - ctx     scopes the conversion for cancellation (a format that streams or
//     calls out may honour it; a pure in-memory pass need not).
//   - lc      receives non-fatal notes (e.g. a modular EDS whose dynamic I/O
//     assemblies were skipped); such notes are logged, not returned.
//   - data    is the raw source file.
//   - options carries format-specific tuning and may be nil.
//
// An error is returned only for unrecoverable input (malformed file, or no
// convertible content).
type ConvertFunc func(ctx context.Context, lc logger.LoggingClient,
	data []byte, options map[string]any) (dtos.DeviceProfile, errors.EdgeX)
