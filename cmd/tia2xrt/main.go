// tia2xrt converts a Siemens TIA Portal V16 data block SCL source export
// to an XRT S7 device profile JSON file. See README.md for usage.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv/tia"
)

func main() {
	var dbNumber int
	flag.IntVar(&dbNumber, "db", 1, "Data block number to embed in resource attributes")
	flag.IntVar(&dbNumber, "d", 1, "Alias for -db")

	var outputFile string
	flag.StringVar(&outputFile, "output", "", "Output JSON file (default: <profile name>.json)")
	flag.StringVar(&outputFile, "o", "", "Alias for -output")

	profileName := flag.String("profile-name", "", "Override profile name (default: block name from source)")

	flag.Usage = func() {
		fmt.Fprint(os.Stderr, "Usage: tia2xrt [flags] <input.scl>\n\n"+
			"Convert a TIA Portal V16 data block SCL source export to an XRT S7 device profile JSON.\n\nFlags:\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	// Flags must precede the input file; Go's flag package stops parsing at the
	// first non-flag argument, so trailing flags would be silently ignored.
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}

	inputFile := flag.Arg(0)
	data, err := os.ReadFile(filepath.Clean(inputFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading '%s': %v\n", inputFile, err)
		os.Exit(1)
	}

	profile, edgexErr := tia.Convert(context.Background(), logger.NewClient("tia2xrt", "INFO"), data, map[string]any{
		tia.OptionDBNumber:    dbNumber,
		tia.OptionProfileName: *profileName,
	})
	if edgexErr != nil {
		fmt.Fprintf(os.Stderr, "Error converting '%s': %v\n", inputFile, edgexErr)
		os.Exit(1)
	}

	out, err := json.MarshalIndent(profile, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	// The profile name is sanitised to [a-zA-Z0-9_-] by the converter, so it
	// cannot escape the working directory.
	if outputFile == "" {
		outputFile = profile.Name + ".json"
	}
	// #nosec G703 -- writing to the path the operator passed via -o is this
	// tool's purpose; there is no untrusted source for it.
	if err := os.WriteFile(filepath.Clean(outputFile), out, 0o600); err != nil {
		fmt.Fprintf(os.Stderr, "Error writing '%s': %v\n", outputFile, err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "Profile written to '%s' (%d resources).\n",
		outputFile, len(profile.DeviceResources))
}
