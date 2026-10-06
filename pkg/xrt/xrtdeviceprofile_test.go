// Copyright (C) 2026 IOTech Ltd

package xrt

import (
	"reflect"
	"testing"
)

func TestAllDeviceProfiles(t *testing.T) {
	tests := []struct {
		name  string
		reply string
	}{
		// Captured from XRT 3.4.6.
		{"objects", `{"client":"c","result":{"profiles":[{"in_use":true,"name":"modbus-sim-profile"},{"in_use":false,"name":"SimpleServer-5"}],"status":0},"type":"xrt.reply:1.0"}`},
		{"strings", `{"client":"c","result":{"profiles":["modbus-sim-profile","SimpleServer-5"],"status":0},"type":"xrt.reply:1.0"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newReplyingClient(t, tt.reply).AllDeviceProfiles(t.Context())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if want := []string{"modbus-sim-profile", "SimpleServer-5"}; !reflect.DeepEqual(got, want) {
				t.Fatalf("got %v, want %v", got, want)
			}
		})
	}
}

func TestAllDeviceProfilesInvalidItem(t *testing.T) {
	reply := `{"client":"c","result":{"profiles":[42],"status":0},"type":"xrt.reply:1.0"}`

	if _, err := newReplyingClient(t, reply).AllDeviceProfiles(t.Context()); err == nil {
		t.Fatal("a profile that is neither a name nor an object must fail")
	}
}
