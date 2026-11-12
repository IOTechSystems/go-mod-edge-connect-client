// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"slices"
	"strings"

	extCommon "github.com/IOTechSystems/go-mod-central-ext/v4/pkg/common"
	"github.com/google/uuid"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
)

// XRT request and reply topic patterns of one device service on a Sparkplug node.
const (
	RequestTopicPattern = namespace + "/" + groupField + "/REQUEST/" + instanceField + "/" +
		extCommon.TopicPatternFieldKeyDeviceServiceName
	ReplyTopicPattern = namespace + "/" + groupField + "/REPLY/" + instanceField + "/" +
		extCommon.TopicPatternFieldKeyDeviceServiceName

	groupField    = "${" + extCommon.TopicPatternFieldGroupName + "}"
	instanceField = "${" + extCommon.TopicPatternFieldInstName + "}"
)

// edgeInstIDNamespace is fixed: changing it changes every edge instance id.
var edgeInstIDNamespace = uuid.MustParse("1a166f0d-27e8-4b65-9e7f-97c7afb4ddf7")

// EdgeInstID is a UUID v5 of (group, node): stable across restarts and reconnects, and URL-safe.
// "/" is an unambiguous separator because Sparkplug group and node IDs must not contain "/", "+" or "#".
func EdgeInstID(k models.NodeKey) string {
	return uuid.NewSHA1(edgeInstIDNamespace, []byte(k.Group+"/"+k.Node)).String()
}

// XrtTopics returns the XRT request and reply topics of deviceService on the node.
func XrtTopics(k models.NodeKey, deviceService string) (request, reply string) {
	r := strings.NewReplacer(
		groupField, k.Group,
		instanceField, k.Node,
		extCommon.TopicPatternFieldKeyDeviceServiceName, deviceService,
	)
	return r.Replace(RequestTopicPattern), r.Replace(ReplyTopicPattern)
}

// IsEdgeInst reports whether the node is offered as an edge instance: it runs at least one device service and every
// one is supported, so one unsupported service hides the whole node. unsupported names the first unsupported service,
// empty when the node runs none.
func IsEdgeInst(n models.NodeInfo) (ok bool, unsupported string) {
	if len(n.Services) == 0 {
		return false, ""
	}
	if i := slices.IndexFunc(n.Services, func(s string) bool { return !xrt.IsSupportedDeviceService(s) }); i >= 0 {
		return false, n.Services[i]
	}
	return true, ""
}
