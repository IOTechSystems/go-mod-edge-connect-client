// Copyright (C) 2026 IOTech Ltd

package eds

import (
	"bytes"
	"context"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
)

// A minimal modular adapter EDS: identity + a fixed status assembly (kept) + two
// ProxyConnect entries and a [Modular] section (the dynamic-I/O markers). Mirrors
// the shape of a real POINT I/O adapter without shipping a vendor EDS file.
const modularAdapterEDS = `
[Device]
    VendName = "Demo Corp";
    ProdName = "DEMO-AENT";

[Params]
    Param1 = 0,,,0x0000,0xC6,1,"Module Count";
    Param2 = 0,,,0x0000,0xC6,1,"Adapter Status";

[Assembly]
    Assem1 =
        "Adapter Status Assembly",
        "20 04 24 01 30 03",
        2, 0x0000, , ,
        8, Param1,
        8, Param2;

[Connection Manager]
    Connection1 =
        0x02010002, 0x44240305,
        , 2, ,
        , 2, Assem1,
        , , , ,
        "Adapter Status Connection";
    ProxyConnect1 =
        0x02010002, 0x44240305, , 0, , , , ,
        "Proxied Digital Input";
    ProxyConnect2 =
        0x02010002, 0x44240305, , 0, , , , ,
        "Proxied Digital Output";

[Modular]
    DefineSlotsInRack = 8;
`

func parseString(t *testing.T, src string) *eds {
	t.Helper()
	e, perr := parse(bytes.NewReader([]byte(src)))
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	return e
}

// isModularOf extracts src and returns the modular flag, failing on error.
func isModularOf(t *testing.T, e *eds) bool {
	t.Helper()
	x, err := extract(logger.NewMockClient(), e)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	return x.isModular
}

// A fixed-I/O EDS is not flagged modular.
func TestExtractModular_FixedIO(t *testing.T) {
	if isModularOf(t, parseSample(t)) {
		t.Error("fixed-I/O sample should not be modular")
	}
}

// A ProxyConnect entry flags the device modular.
func TestExtractModular_ProxyConnect(t *testing.T) {
	if !isModularOf(t, parseString(t, modularAdapterEDS)) {
		t.Error("adapter with ProxyConnect entries should be modular")
	}
}

// A [Modular] section alone (no ProxyConnect) flags the device modular.
func TestExtractModular_SectionOnly(t *testing.T) {
	if !isModularOf(t, parseString(t, "[Modular]\n    DefineSlotsInRack = 4;\n")) {
		t.Error("a [Modular] section alone should be modular")
	}
}

// End-to-end: a modular adapter still converts to a valid profile — its fixed
// status assembly and identity survive; the dynamic proxied I/O is absent, with
// warnings logged. It must not error or panic.
func TestConvertModularAdapter(t *testing.T) {
	profile, cerr := Convert(context.Background(), logger.NewMockClient(), []byte(modularAdapterEDS), nil)
	if cerr != nil {
		t.Fatalf("Convert modular adapter: %v", cerr)
	}
	var hasStatus bool
	for _, r := range profile.DeviceResources {
		if r.Name == "Adapter Status" {
			hasStatus = true
		}
	}
	if !hasStatus {
		t.Error("expected the fixed 'Adapter Status' resource to survive")
	}
}
