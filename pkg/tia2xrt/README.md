tia2xrt converts a Siemens TIA Portal V16 data block SCL source export
to an XRT S7 device profile JSON file.

Usage:

	tia2xrt <input_filename> [flags]

Flags:

	-d, -db N          Data block number (default 1)
	-o, -output FILE   Output JSON file (default: stdout)
	-profile-name NAME Override profile name (default: block name from source)
	-allow-optimized   Suppress error for optimised-access blocks

Examples:
    tia2xrt MyInput.txt -db 5 -o profile.json
    tia2xrt MyBlock.db -db 2 -profile-name mixing_db
