// Copyright (C) 2026 IOTech Ltd

package interfaces

import (
	"context"
	"time"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
)

// SparkplugClient observes the XRT edge nodes of whole Sparkplug groups and keeps their state in memory.
type SparkplugClient interface {
	// Rebirth publishes Node Control/Rebirth to spBv1.0/<group>/NCMD for every group; it does not wait for replies.
	Rebirth() errors.EdgeX
	// Nodes return the online nodes sorted by group, then node.
	Nodes() []models.NodeInfo

	// ReadDeviceResources and WriteDeviceResources are reserved for DCMD/DACK and return KindNotImplemented for now.
	ReadDeviceResources(ctx context.Context, node models.NodeKey, device string,
		resourceNames []string) (xrtmodels.MultiResourcesResult, errors.EdgeX)
	WriteDeviceResources(ctx context.Context, node models.NodeKey, device string,
		resourceValuePairs, options map[string]any) errors.EdgeX

	// SetRequestTimeout sets how long a DCMD waits for its DACK; unused until DCMD is implemented.
	SetRequestTimeout(requestTimeout time.Duration)
	// Close stops message handling and unsubscribes. The message bus stays connected because it is shared; call Close
	// before disconnecting it.
	Close() errors.EdgeX
}
