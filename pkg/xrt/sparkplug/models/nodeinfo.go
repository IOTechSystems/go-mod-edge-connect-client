// Copyright (C) 2026 IOTech Ltd

package models

// NodeKey identifies an edge node; node IDs are unique only within a group.
type NodeKey struct {
	Group string
	Node  string
}

// NodeInfo is the state of one XRT edge node, taken from its latest NBIRTH and the DBIRTH/DDEATH that follow.
// Its slices are replaced, never modified in place, so callers must not modify them either.
type NodeInfo struct {
	NodeKey
	BdSeq    int64    // an NDEATH is applied only if it carries the same or a newer value
	Services []string // config.Name of each NBIRTH Service/* metric with category "XRT::DeviceService"
	// Components are the XRT components the NBIRTH Service/* metrics list; empty unless XRT EnableServices is on.
	Components []Component
	Metrics    []MetricDef // NBIRTH
	Devices    []Device    // online devices (DBIRTH); cleared on every NBIRTH
}

// Component is one XRT component of a node, as its NBIRTH Service/* metric describes it. Its config is not kept: the
// bridge holds the broker credentials in clear text.
type Component struct {
	Name     string
	Category string // e.g. "XRT::DeviceService"
	Type     string // e.g. "XRT::ModbusDeviceService"
	State    string // e.g. "Running", at the NBIRTH
}

// Device is one device of a node, taken from its latest DBIRTH.
type Device struct {
	Name    string
	Profile string // value of the Properties/Profile metric
	Metrics []MetricDef
}

// MetricDef is one metric definition from an NBIRTH or DBIRTH.
type MetricDef struct {
	Name      string
	Alias     uint64 // 0 if the metric has none (e.g. bdSeq)
	Datatype  uint32 // Sparkplug DataType
	ReadWrite string // readWrite property: "R", "W", "RW", or empty
	// ContentType is the metadata content_type, e.g. "xrt/json-object" for an EdgeX Object resource
	ContentType string
}
