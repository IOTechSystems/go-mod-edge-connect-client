// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// CIP logical segment types (the high bits of a segment byte). The low 2 bits
// are the format: 0 = 8-bit value, 1 = 16-bit (little-endian), 2 = 32-bit.
const (
	segClass        = 0x20 // class id
	segInstance     = 0x24 // instance id
	segAttribute    = 0x30 // attribute id
	segConnectionPt = 0x2C // connection point (an instance variant)
	segFormatMask   = 0x03 // low 2 bits: value width (0=8-bit, 1=16-bit, 2=32-bit)
)

// epathSegment is one decoded CIP logical segment: its logical type (segClass /
// segInstance / segAttribute / segConnectionPt, with the format bits cleared)
// and its value.
type epathSegment struct {
	logicalType uint64
	value       int
}

// parseEPATH decodes a CIP EPATH such as "20 04 24 64 30 03" into its logical
// segments. It walks segment by segment — not token by token —
// so a value byte that happens to equal a segment code is never mistaken for a
// segment type. The format bits select the value width (8/16/32-bit,
// little-endian). Returns an error on a malformed path (bad byte, truncated
// value, or an unrecognised segment type).
//
// EDS paths use the CIP *padded* logical-segment encoding: a 16-bit or 32-bit
// value is preceded by a single 0x00 pad byte (for 16-bit-word alignment), so
// e.g. instance 256 is "25 00 00 01", not "25 00 01". An 8-bit value has no pad
// (ODVA CIP Networks Library Vol.1, Appendix C).
func parseEPATH(path string) ([]epathSegment, errors.EdgeX) {
	toks := strings.Fields(strings.Trim(strings.TrimSpace(path), `"`))

	var segs []epathSegment
	for i := 0; i < len(toks); {
		seg, consumed, err := decodeEPATHSegment(toks, i, path)
		if err != nil {
			return nil, err
		}
		segs = append(segs, seg)
		i += consumed
	}
	return segs, nil
}

// decodeEPATHSegment decodes the logical segment starting at toks[i] and reports
// how many tokens it consumed (type byte + optional pad + value bytes).
func decodeEPATHSegment(toks []string, i int, path string) (epathSegment, int, errors.EdgeX) {
	readByte := func(j int) (uint64, errors.EdgeX) {
		v, err := strconv.ParseUint(toks[j], 16, 8)
		if err != nil {
			return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("invalid EPATH byte %q in path %q", toks[j], path), err)
		}
		return v, nil
	}

	seg, err := readByte(i)
	if err != nil {
		return epathSegment{}, 0, err
	}
	logicalType := seg &^ segFormatMask
	format := seg & segFormatMask
	if format == 3 { // 0/1/2 = 8/16/32-bit; 3 is reserved (invalid)
		return epathSegment{}, 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("EPATH segment 0x%02X has a reserved size format in path %q", seg, path), nil)
	}
	switch logicalType {
	case segClass, segInstance, segAttribute, segConnectionPt:
	default:
		return epathSegment{}, 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("unexpected EPATH segment 0x%02X in path %q", seg, path), nil)
	}

	width := 1 << format // value byte count: 1/2/4
	// Padded encoding: 16/32-bit values carry one 0x00 pad byte after the
	// segment-type byte, so the value starts at i+1+pad.
	pad := 0
	if width > 1 {
		pad = 1
	}
	// value bytes are toks[i+1+pad .. i+pad+width]; need that within bounds.
	if i+pad+width >= len(toks) {
		return epathSegment{}, 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("EPATH segment 0x%02X without its %d value byte(s): %q", seg, width, path), nil)
	}

	// The pad byte is defined as 0x00; a non-zero pad is a malformed path, not a
	// value byte to fold in, so reject it rather than silently ignoring it.
	if pad == 1 {
		p, err := readByte(i + 1)
		if err != nil {
			return epathSegment{}, 0, err
		}
		if p != 0 {
			return epathSegment{}, 0, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("EPATH segment 0x%02X has a non-zero pad byte 0x%02X in path %q", seg, p, path), nil)
		}
	}

	var value uint64
	for b := 0; b < width; b++ {
		vb, err := readByte(i + 1 + pad + b)
		if err != nil {
			return epathSegment{}, 0, err
		}
		value |= vb << (8 * b) // little-endian
	}
	return epathSegment{logicalType: logicalType, value: int(value)}, 1 + pad + width, nil
}
