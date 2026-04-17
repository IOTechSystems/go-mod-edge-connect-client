// Copyright (C) 2023-2026 IOTech Ltd

package xrt

import (
	"context"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func (c *Client) TriggerDiscovery(ctx context.Context) errors.EdgeX {
	if c.clientOptions == nil || c.clientOptions.DiscoveryOptions == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "please provide DiscoveryOptions for the discovery request", nil)
	}

	c.lc.Debugf("triggering discovery with discovery options - %v", c.clientOptions.ExtendedDiscoveryOptions)
	request := xrtmodels.NewDiscoveryRequest(clientName, c.clientOptions.ExtendedDiscoveryOptions)
	var response xrtmodels.CommonResponse

	err := c.sendXrtDiscoveryRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to trigger discovery", err)
	}
	return nil
}
