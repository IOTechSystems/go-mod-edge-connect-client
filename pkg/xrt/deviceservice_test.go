// Copyright (C) 2026 IOTech Ltd

package xrt

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A node is skipped whole when one of its device services is unsupported, so a typo here hides every node running it.
func TestIsSupportedDeviceService(t *testing.T) {
	// connect-ui's list before this code moved here (edge-ui-combo pkg/common/edgeinst.go).
	want := []string{"bacnet_ip", "bacnet_mstp", "ble", "canbus", "ethernet_ip", "gps", "modbus", "opc_ua", "s7", "virtual"}
	for _, name := range want {
		assert.True(t, IsSupportedDeviceService(name), name)
	}
	assert.False(t, IsSupportedDeviceService("device-modbus"))
	assert.False(t, IsSupportedDeviceService("virutal"), "a typo is not supported")
	assert.False(t, IsSupportedDeviceService(""))
}
