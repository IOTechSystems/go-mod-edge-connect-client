tia2xrt converts a Siemens TIA Portal V16 data block SCL source export
to an XRT S7 device profile JSON file.

Build:

	go build -o tia2xrt ./cmd/tia2xrt

Usage:

	tia2xrt [flags] <input.scl>

Flags:

	-d, -db N          Data block number (default 1)
	-o, -output FILE   Output JSON file (default: <profile name>.json)
	-profile-name NAME Override profile name (default: block name from source)

Optimized-access data blocks are rejected: TIA Portal decides their layout
internally, so byte offsets cannot be derived from the declaration order. Turn
off "Optimized block access" in the block properties and re-export.

Examples:

	tia2xrt -db 5 -o profile.json MyBlock.scl
	tia2xrt -db 2 -profile-name mixing_db MyBlock.scl   # writes mixing_db.json
