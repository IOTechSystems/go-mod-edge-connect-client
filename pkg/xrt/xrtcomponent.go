// Copyright (C) 2023-2025 IOTech Ltd

package xrt

import (
	"context"
	"time"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

const (
	luaTransformComponent = "lua"
	componentConfigScript = "Script"
)

func (c *Client) UpdateLuaScript(ctx context.Context, luaScript string) errors.EdgeX {
	config := map[string]interface{}{
		componentConfigScript: luaScript,
	}
	request := xrtmodels.NewComponentUpdateRequest(luaTransformComponent, clientName, config)
	var response xrtmodels.CommonResponse

	err := c.sendXrtCommandRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to update the Lua script to Lua transform component", err)
	}
	return nil
}

func (c *Client) DiscoverComponents(ctx context.Context, category string, subscribeTimeout time.Duration) ([]xrtmodels.MultiComponentsResponse, errors.EdgeX) {
	request := xrtmodels.NewComponentDiscoverRequest(clientName, category)
	var response []xrtmodels.MultiComponentsResponse

	err := c.sendXrtRequestWithSubTimeout(ctx, c.requestTopic, request.RequestId, request, &response, subscribeTimeout)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to discover the xrt components", err)
	}

	return response, nil
}

func (c *Client) UpdateComponent(ctx context.Context, name string, config map[string]any) errors.EdgeX {
	request := xrtmodels.NewComponentUpdateRequest(name, clientName, config)
	var response xrtmodels.CommonResponse

	err := c.sendXrtCommandRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to update the component", err)
	}
	return nil
}
