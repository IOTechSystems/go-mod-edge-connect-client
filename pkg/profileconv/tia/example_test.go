// Copyright (C) 2026 IOTech Ltd

package tia_test

import (
	"context"
	"fmt"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/profileconv/tia"
)

// ExampleConvert converts one data block export. It runs from outside the
// package, so it also checks that Convert plus the Option constants are the whole
// public API a caller needs, and that Convert really does satisfy
// profileconv.ConvertFunc when used through that type.
//
// The data block number is a TIA project property, absent from the export, so the
// caller supplies it.
func ExampleConvert() {
	sclData := []byte(`
DATA_BLOCK "Motor_Data"
{ S7_Optimized_Access := 'FALSE' }
VERSION : 0.1
   STRUCT
      Speed : Int;
      Running : Bool;
      Name : String[20];
   END_STRUCT;
BEGIN
END_DATA_BLOCK
`)

	var convert profileconv.ConvertFunc = tia.Convert
	profile, err := convert(context.Background(), logger.NewMockClient(), sclData,
		map[string]any{tia.OptionDBNumber: 5})
	if err != nil {
		fmt.Println("convert failed:", err)
		return
	}

	// The offsets themselves are asserted in convert_test.go; here the point is
	// that a caller gets a usable profile out.
	fmt.Println(profile.Name, len(profile.DeviceResources), "resources")

	// Output:
	// Motor_Data 3 resources
}
