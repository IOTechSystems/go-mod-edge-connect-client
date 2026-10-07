// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"fmt"
	"strings"
	"time"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

const (
	namespace = "spBv1.0"

	msgTypeNBIRTH = "NBIRTH"
	msgTypeNDEATH = "NDEATH"
	msgTypeDBIRTH = "DBIRTH"
	msgTypeDDEATH = "DDEATH"
	msgTypeNCMD   = "NCMD"
	// msgTypeDACK is XRT's acknowledgement of a DCMD; it is not part of the Sparkplug B spec.
	msgTypeDACK = "DACK"

	bdSeqMetricName       = "bdSeq"
	nodeRebirthMetricName = "Node Control/Rebirth"
)

// parseTopic splits a Sparkplug topic, namespace/group_id/message_type/edge_node_id/[device_id] (spec 3.0.0, 4.1).
func parseTopic(topic string) (group, msgType, node, device string, err error) {
	elements := strings.Split(topic, "/")
	switch len(elements) {
	case 4:
		return elements[1], elements[2], elements[3], "", nil
	case 5:
		return elements[1], elements[2], elements[3], elements[4], nil
	}
	return "", "", "", "", fmt.Errorf("topic %s does not comply with the format of sparkplug topic: spBv1.0/group_id/message_type/edge_node_id/[device_id]", topic)
}

// newNodeRebirth returns the NCMD payload that requests a node rebirth. An NCMD carries a timestamp and no seq
// (spec 3.0.0, 6.4.23).
func newNodeRebirth() *protobuf.Payload {
	ts := uint64(time.Now().UnixMilli())
	name, dataType := nodeRebirthMetricName, uint32(protobuf.DataType_Boolean)
	return &protobuf.Payload{
		Timestamp: &ts,
		Metrics: []*protobuf.Payload_Metric{{
			Name:      &name,
			Timestamp: &ts,
			Datatype:  &dataType,
			Value:     &protobuf.Payload_Metric_BooleanValue{BooleanValue: true},
		}},
	}
}
