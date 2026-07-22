// tia2xrt converts a Siemens TIA Portal V16 data block SCL source export
// to an XRT S7 device profile JSON file.

// Usage:

// 	tia2xrt <input_filename> [flags]

// Flags:

// -d, -db N          Data block number (default 1)
// -o, -output FILE   Output JSON file (default: stdout)
// -profile-name NAME Override profile name (default: block name from source)
// -allow-optimized   Suppress error for optimised-access blocks
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/tia2xrt"
)

func main() {

	if len(os.Args) < 1 {
		flag.Usage()
		os.Exit(1)
	}

	var dbNumber int
	flag.IntVar(&dbNumber, "db", 1, "Data block number to embed in resource attributes")
	flag.IntVar(&dbNumber, "d", 1, "Alias for -db")

	var outputFile string
	flag.StringVar(&outputFile, "output", "", "Output JSON file (default: stdout)")
	flag.StringVar(&outputFile, "o", "", "Alias for -output")

	profileName := flag.String("profile-name", "", "Override profile name (default: block name from source)")
	allowOptimized := flag.Bool("allow-optimized", false,
		"Continue even if optimized access is detected (byte offsets will be wrong)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: tia2xrt [flags] <input.scl>\n\n"+
			"Convert a TIA Portal V16 data block SCL source export to an XRT S7 device profile JSON.\n\nFlags:\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n"+
			"  tia2xrt MyBlock.scl -db 5 -o profile.json\n"+
			"  tia2xrt MyBlock.scl -db 2 -profile-name mixing_db\n")
	}

	err := flag.CommandLine.Parse(os.Args[2:])

	if err != nil {
		flag.Usage()
		os.Exit(1)
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(1)
	}

	inputFile := flag.Arg(0)
	data, err := os.ReadFile(filepath.Clean(inputFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading '%s': %v\n", inputFile, err)
		os.Exit(1)
	}

	blockName, isOptimized, variables := tia2xrt.ParseSCL(string(data))

	if isOptimized && !*allowOptimized {
		fmt.Fprintln(os.Stderr, "Error: block has S7_Optimized_Access := 'TRUE'.")
		fmt.Fprintln(os.Stderr, "Optimized blocks have no fixed absolute byte offsets; S7 symbolic addressing is not supported by XRT.")
		fmt.Fprintln(os.Stderr, "Disable optimized access in TIA Portal (block properties → 'Optimized block access' → off) and re-export,")
		fmt.Fprintln(os.Stderr, "or pass -allow-optimized to suppress this error.")
		os.Exit(1)
	}
	if isOptimized {
		fmt.Fprintln(os.Stderr, "Warning: optimized-access block – byte offsets will be incorrect.")
	}

	name := *profileName
	if name == "" {
		name = regexp.MustCompile(`[^a-zA-Z0-9_\-]`).ReplaceAllString(blockName, "_")
	}

	profile, warnings := tia2xrt.BuildProfile(name, dbNumber, variables)

	for _, w := range warnings {
		fmt.Fprintf(os.Stderr, "Warning: %s\n", w)
	}
	if len(profile.DeviceResources) == 0 {
		fmt.Fprintln(os.Stderr, "Warning: no device resources were generated – check the input file.")
	}

	out, err := tia2xrt.MarshalProfile(profile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	if outputFile != "" {
		if err := os.WriteFile(outputFile, out, 0o600); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing '%s': %v\n", outputFile, err)
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "Profile written to '%s' (%d resources).\n",
			outputFile, len(profile.DeviceResources))
	} else {
		fmt.Println(string(out))
	}
}
