// Copyright (C) 2023-2024 IOTech Ltd

package xrt

import (
	"context"
	"fmt"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/xrtmodels"
)

func (c *Client) AllDeviceProfiles(ctx context.Context) ([]string, errors.EdgeX) {
	request := xrtmodels.NewAllProfilesRequest(clientName)
	var response xrtmodels.MultiProfilesResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to query profile list", err)
	}
	return response.Result.Profiles, nil
}

func (c *Client) DeviceProfileByName(ctx context.Context, name string) (dtos.DeviceProfile, errors.EdgeX) {
	request := xrtmodels.NewProfileGetRequest(name, clientName)
	var response xrtmodels.ProfileResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return dtos.DeviceProfile{}, errors.NewCommonEdgeX(errors.Kind(err), "failed to query profile", err)
	}
	return response.Result.Profile, nil
}

func (c *Client) AddDeviceProfile(ctx context.Context, profile dtos.DeviceProfile) errors.EdgeX {
	request := xrtmodels.NewProfileAddRequest(profile, clientName)
	var response xrtmodels.CommonResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to add profile", err)
	}
	return nil
}

func (c *Client) UpdateDeviceProfile(ctx context.Context, profile dtos.DeviceProfile) errors.EdgeX {
	request := xrtmodels.NewProfileUpdateRequest(profile, clientName)
	var response xrtmodels.CommonResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to update profile", err)
	}
	return nil
}

func (c *Client) DeleteDeviceProfileByName(ctx context.Context, name string) errors.EdgeX {
	request := xrtmodels.NewProfileDeleteRequest(name, clientName)
	var response xrtmodels.CommonResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), fmt.Sprintf("failed to delete profile %s", name), err)
	}
	return nil
}
