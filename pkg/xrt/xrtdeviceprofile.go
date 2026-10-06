// Copyright (C) 2023-2024 IOTech Ltd

package xrt

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

// profileListResponse replaces xrtmodels.MultiProfilesResponse, whose []string profiles cannot decode XRT 3.4,
// which lists each profile as {"name", "in_use"}.
type profileListResponse struct {
	xrtmodels.BaseResponse `json:",inline"`
	Result                 struct {
		xrtmodels.BaseResult `json:",inline"`
		Profiles             []profileListItem `json:"profiles"`
	} `json:"result"`
}

// profileListItem accepts both a bare profile name and a {"name": ..., "in_use": ...} object.
type profileListItem struct {
	Name string `json:"name"`
}

func (item *profileListItem) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &item.Name); err == nil {
		return nil
	}
	type object profileListItem
	return json.Unmarshal(data, (*object)(item))
}

func (c *Client) AllDeviceProfiles(ctx context.Context) ([]string, errors.EdgeX) {
	request := xrtmodels.NewAllProfilesRequest(clientName)
	var response profileListResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to query profile list", err)
	}
	names := make([]string, 0, len(response.Result.Profiles))
	for _, profile := range response.Result.Profiles {
		names = append(names, profile.Name)
	}
	return names, nil
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
