// Copyright (C) 2026 IOTech Ltd

package eds_test

import (
	"context"
	"fmt"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv/eds"
)

// ExampleConvert shows converting one EtherNet/IP EDS into an EdgeX
// DeviceProfile. eds.Convert satisfies profileconv.ConvertFunc, so it can be
// used directly or through the format-neutral type.
func ExampleConvert() {
	edsData := []byte(`
[Device]
    VendName = "Demo Corp";
    ProdName = "DEMO-DIO8";

[Params]
    Param1 = 0,,,0x0000,0xC1,1,"DO0";

[Assembly]
    Assem100 = "Outputs", "20 04 24 64 30 03", 1, 0x0000, , , 1, Param1;

[Connection Manager]
    Connection1 = 0x04020002, 0x44640405, , 1, Assem100, , , ;
`)

	var convert profileconv.ConvertFunc = eds.Convert
	profile, err := convert(context.Background(), logger.NewMockClient(), edsData, nil)
	if err != nil {
		fmt.Println("convert failed:", err)
		return
	}
	fmt.Println("name:", profile.Name)
	fmt.Println("model:", profile.Model)

	// Output:
	// name: demo-dio8
	// model: DEMO-DIO8
}
