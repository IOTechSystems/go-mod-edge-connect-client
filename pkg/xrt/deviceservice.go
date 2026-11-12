// Copyright (C) 2026 IOTech Ltd

package xrt

import "slices"

// Device services an edge instance may run; a node running any other is not offered as an edge instance.
const (
	DeviceServiceBACnetIP   = "bacnet_ip"
	DeviceServiceBACnetMSTP = "bacnet_mstp"
	DeviceServiceBLE        = "ble"
	DeviceServiceCANbus     = "canbus"
	DeviceServiceEtherNetIP = "ethernet_ip"
	DeviceServiceGPS        = "gps"
	DeviceServiceModbus     = "modbus"
	DeviceServiceOPCUA      = "opc_ua"
	DeviceServiceS7         = "s7"
	DeviceServiceVirtual    = "virtual"
)

var supportedDeviceServices = []string{
	DeviceServiceBACnetIP, DeviceServiceBACnetMSTP, DeviceServiceBLE,
	DeviceServiceCANbus, DeviceServiceEtherNetIP, DeviceServiceGPS,
	DeviceServiceModbus, DeviceServiceOPCUA, DeviceServiceS7, DeviceServiceVirtual,
}

// IsSupportedDeviceService reports whether an edge instance may run the device service.
func IsSupportedDeviceService(name string) bool {
	return slices.Contains(supportedDeviceServices, name)
}
