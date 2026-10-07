// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"encoding/json"
	"strings"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

const (
	serviceMetricPrefix   = "Service/"
	deviceServiceCategory = "XRT::DeviceService"
	readWriteProperty     = "readWrite"
)

// xrtComponent is the JSON value of an NBIRTH Service/<component> metric, published when XRT EnableServices is on.
type xrtComponent struct {
	Category string `json:"category"`
	Config   struct {
		Name string `json:"Name"`
	} `json:"config"`
}

// bdSeq returns the bdSeq metric carried by an NBIRTH or NDEATH.
func bdSeq(payload *protobuf.Payload) (int64, bool) {
	for _, m := range payload.GetMetrics() {
		if m.GetName() == bdSeqMetricName {
			return int64(m.GetLongValue()), true
		}
	}
	return 0, false
}

// isSameOrNewerBdSeq reports whether an NDEATH bdSeq belongs to the stored session or to a later one whose NBIRTH
// was missed. bdSeq wraps at 255, so the distance is taken modulo 256 and up to 127 counts as newer.
func isSameOrNewerBdSeq(stored, death int64) bool {
	distance := (death - stored) % 256
	if distance < 0 {
		distance += 256
	}
	return distance < 128
}

// deviceServices returns config.Name of each device service listed in the NBIRTH Service/* metrics, and the names
// of the Service/* metrics it could not parse, which are skipped.
func deviceServices(payload *protobuf.Payload) (names, invalid []string) {
	for _, m := range payload.GetMetrics() {
		if !strings.HasPrefix(m.GetName(), serviceMetricPrefix) {
			continue
		}
		var comp xrtComponent
		if err := json.Unmarshal([]byte(m.GetStringValue()), &comp); err != nil {
			invalid = append(invalid, m.GetName())
			continue
		}
		if comp.Category == deviceServiceCategory && comp.Config.Name != "" {
			names = append(names, comp.Config.Name)
		}
	}
	return names, invalid
}

func metricDefs(payload *protobuf.Payload) []models.MetricDef {
	defs := make([]models.MetricDef, 0, len(payload.GetMetrics()))
	for _, m := range payload.GetMetrics() {
		defs = append(defs, models.MetricDef{
			Name:      m.GetName(),
			Alias:     m.GetAlias(),
			Datatype:  m.GetDatatype(),
			ReadWrite: stringProperty(m.GetProperties(), readWriteProperty),
		})
	}
	return defs
}

func stringProperty(props *protobuf.Payload_PropertySet, key string) string {
	for i, k := range props.GetKeys() {
		if k == key && i < len(props.GetValues()) {
			return props.GetValues()[i].GetStringValue()
		}
	}
	return ""
}
