// Copyright (C) 2023-2024 IOTech Ltd

package xrt

import (
	"context"
	"fmt"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/xrtmodels"
)

func (c *Client) AllDevices(ctx context.Context) ([]string, errors.EdgeX) {
	request := xrtmodels.NewAllDevicesRequest(clientName)
	var response xrtmodels.MultiDevicesResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to query device list", err)
	}
	return response.Result.Devices, nil
}

func (c *Client) DeviceByName(ctx context.Context, name string) (xrtmodels.DeviceInfo, errors.EdgeX) {
	request := xrtmodels.NewDeviceGetRequest(name, clientName)
	var response xrtmodels.DeviceResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return xrtmodels.DeviceInfo{}, errors.NewCommonEdgeX(errors.Kind(err), "failed to query device", err)
	}
	return response.Result.Device, nil
}

func (c *Client) AddDevice(ctx context.Context, device dtos.Device) errors.EdgeX {
	xrtDevice, err := xrtmodels.ToXrtDevice(device)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to convert Edgex device to XRT device data", err)
	}
	request := xrtmodels.NewDeviceAddRequest(xrtDevice, clientName)
	var response xrtmodels.CommonResponse

	err = c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to add device", err)
	}
	return nil
}

func (c *Client) UpdateDevice(ctx context.Context, device dtos.Device) errors.EdgeX {
	xrtDevice, err := xrtmodels.ToXrtDevice(device)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to convert Edgex device to XRT device data", err)
	}
	request := xrtmodels.NewDeviceUpdateRequest(xrtDevice, clientName)
	var response xrtmodels.CommonResponse

	err = c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to update device", err)
	}
	return nil
}

func (c *Client) DeleteDeviceByName(ctx context.Context, name string) errors.EdgeX {
	request := xrtmodels.NewDeviceDeleteRequest(name, clientName)
	var response xrtmodels.CommonResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), fmt.Sprintf("failed to delete device %s", name), err)
	}
	return nil
}

// AddDiscoveredDevice adds discovered device without profile, which means the device is not usable until the profile is set or generate by device:scan
func (c *Client) AddDiscoveredDevice(ctx context.Context, device dtos.Device) errors.EdgeX {
	xrtDevice, err := xrtmodels.ToXrtDevice(device)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to convert Edgex device to XRT device data", err)
	}
	request := xrtmodels.NewDiscoveredDeviceAddRequest(xrtDevice, clientName)
	var response xrtmodels.CommonResponse

	err = c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to add discovered device", err)
	}
	return nil
}

// ScanDevice checks a device profile for updates.
func (c *Client) ScanDevice(ctx context.Context, device dtos.Device, options map[string]any) errors.EdgeX {
	xrtDevice, err := xrtmodels.ToXrtDevice(device)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to convert Edgex device to XRT device data", err)
	}
	request := xrtmodels.NewDeviceScanRequest(xrtDevice, clientName, options)
	c.lc.Debugf("Sending device scan request for device %s with profile name %s and options %v", request.DeviceName, request.ProfileName, request.Options)
	var response xrtmodels.CommonResponse

	// use discovery request for auto-generate or updating the profile
	err = c.sendXrtDiscoveryRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to scan device", err)
	}
	return nil
}

func (c *Client) ReadDeviceResources(ctx context.Context, deviceName string, resourceNames []string) (xrtmodels.MultiResourcesResult, errors.EdgeX) {
	request := xrtmodels.NewDeviceResourceGetRequest(deviceName, clientName, resourceNames)
	var response xrtmodels.MultiResourcesResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeX(errors.Kind(err), "failed to read device resources", err)
	}
	return response.Result, nil
}

func (c *Client) WriteDeviceResources(ctx context.Context, deviceName string, resourceValuePairs, options map[string]any) errors.EdgeX {
	request := xrtmodels.NewDeviceResourceSetRequest(deviceName, clientName, resourceValuePairs, options)
	var response xrtmodels.CommonResponse

	err := c.sendXrtRequest(ctx, request.RequestId, request, &response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to write device resources", err)
	}
	return nil
}
