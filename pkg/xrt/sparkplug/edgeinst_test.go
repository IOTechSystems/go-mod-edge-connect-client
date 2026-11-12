// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
)

// The ids connect-ui issued before this code moved here: they are stored and shown to users, so they must not change.
func TestEdgeInstID(t *testing.T) {
	tests := []struct {
		key  models.NodeKey
		want string
	}{
		{models.NodeKey{Group: "iotech", Node: "xrt-modbus-v3.4"}, "86a5ad05-ac42-5d6a-8c0a-97be6776dd72"}, // connect-ui's own test
		{models.NodeKey{Group: "iotech", Node: "edge-01"}, "b9fb9cdc-693b-5a0c-9990-ecba60138212"},
		{models.NodeKey{Group: "site-b", Node: "node.2"}, "e94b979a-e4dc-593c-8cf3-1a6b1bd83b5b"},
		{models.NodeKey{Group: "g", Node: "n"}, "305e9352-83b8-5e9f-b9e3-8c8d7f77d5eb"},
	}
	for _, tt := range tests {
		t.Run(tt.key.Group+"/"+tt.key.Node, func(t *testing.T) {
			assert.Equal(t, tt.want, EdgeInstID(tt.key))
		})
	}
}

// Without the separator, ("g1", "n") and ("g", "1n") would hash the same input.
func TestEdgeInstID_DistinctKeys(t *testing.T) {
	assert.NotEqual(t, EdgeInstID(models.NodeKey{Group: "g1", Node: "n"}), EdgeInstID(models.NodeKey{Group: "g", Node: "1n"}))
	assert.NotEqual(t, EdgeInstID(models.NodeKey{Group: "g", Node: "n"}), EdgeInstID(models.NodeKey{Group: "other", Node: "n"}))
}

func TestTopicPatterns(t *testing.T) {
	// The literal patterns connect-ui stored in each edge instance before this code moved here.
	assert.Equal(t, "spBv1.0/${GROUP_NAME}/REQUEST/${INSTANCE_NAME}/${DEVICE_SERVICE_NAME}", RequestTopicPattern)
	assert.Equal(t, "spBv1.0/${GROUP_NAME}/REPLY/${INSTANCE_NAME}/${DEVICE_SERVICE_NAME}", ReplyTopicPattern)
}

func TestXrtTopics(t *testing.T) {
	request, reply := XrtTopics(models.NodeKey{Group: "iotech", Node: "edge-01"}, "modbus")
	assert.Equal(t, "spBv1.0/iotech/REQUEST/edge-01/modbus", request)
	assert.Equal(t, "spBv1.0/iotech/REPLY/edge-01/modbus", reply)
}

func TestIsEdgeInst(t *testing.T) {
	tests := []struct {
		name            string
		services        []string
		wantOK          bool
		wantUnsupported string
	}{
		{"no services", nil, false, ""},
		{"all supported", []string{"modbus", "opc_ua"}, true, ""},
		{"one unsupported", []string{"modbus", "foo"}, false, "foo"},
		{"first unsupported is named", []string{"foo", "modbus", "bar"}, false, "foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, unsupported := IsEdgeInst(models.NodeInfo{NodeKey: models.NodeKey{Group: "g", Node: "n"}, Services: tt.services})
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantUnsupported, unsupported)
		})
	}
}
