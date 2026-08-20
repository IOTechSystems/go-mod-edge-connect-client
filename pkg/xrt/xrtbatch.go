// Copyright (C) 2026 IOTech Ltd

package xrt

import (
	"context"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
)

func (c *Client) BatchReadAllDevices(ctx context.Context) ([]*xrtmodels.DeviceInfo, errors.EdgeX) {
	return c.batchReadDevices(ctx, nil, "")
}

func (c *Client) BatchReadDevicesByNames(ctx context.Context, names []string) ([]*xrtmodels.DeviceInfo, errors.EdgeX) {
	if len(names) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one device name is required", nil)
	}
	return c.batchReadDevices(ctx, names, "")
}

func (c *Client) BatchReadDevicesByPattern(ctx context.Context, pattern string) ([]*xrtmodels.DeviceInfo, errors.EdgeX) {
	if pattern == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "pattern is required", nil)
	}
	return c.batchReadDevices(ctx, nil, pattern)
}

func (c *Client) BatchAddDevices(ctx context.Context, devices []dtos.Device) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	if len(devices) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one device is required", nil)
	}

	xrtDevices := make([]xrtmodels.DeviceInfo, 0, len(devices))
	for _, device := range devices {
		xrtDevice, err := xrtmodels.ToXrtDevice(device)
		if err != nil {
			return nil, errors.NewCommonEdgeX(errors.KindServerError, convertErrMsg, err)
		}
		xrtDevices = append(xrtDevices, xrtDevice)
	}

	request := xrtmodels.NewBatchAddDevicesRequest(xrtDevices, clientName)
	return c.sendBatchDeviceRequest(ctx, request, request.RequestId, "failed to add the devices")
}

func (c *Client) BatchDeleteDevices(ctx context.Context, names []string) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	if len(names) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one device name is required", nil)
	}

	request := xrtmodels.NewBatchDeleteDevicesRequest(names, clientName)
	return c.sendBatchDeviceRequest(ctx, request, request.RequestId, "failed to delete the devices")
}

func (c *Client) BatchReadAllSchedules(ctx context.Context) ([]*xrtmodels.Schedule, errors.EdgeX) {
	return c.batchReadSchedules(ctx, nil, "", "")
}

func (c *Client) BatchReadSchedulesByNames(ctx context.Context, names []string) ([]*xrtmodels.Schedule, errors.EdgeX) {
	if len(names) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one schedule name is required", nil)
	}
	return c.batchReadSchedules(ctx, names, "", "")
}

func (c *Client) BatchReadSchedulesByDevice(ctx context.Context, deviceName string) ([]*xrtmodels.Schedule, errors.EdgeX) {
	if deviceName == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "device name is required", nil)
	}
	return c.batchReadSchedules(ctx, nil, deviceName, "")
}

func (c *Client) BatchReadSchedulesByPattern(ctx context.Context, pattern string) ([]*xrtmodels.Schedule, errors.EdgeX) {
	if pattern == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "pattern is required", nil)
	}
	return c.batchReadSchedules(ctx, nil, "", pattern)
}

func (c *Client) BatchAddSchedules(ctx context.Context, schedules []xrtmodels.Schedule) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	if len(schedules) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one schedule is required", nil)
	}

	request := xrtmodels.NewBatchAddSchedulesRequest(schedules, clientName)
	return c.sendBatchScheduleRequest(ctx, request, request.RequestId, "failed to add the schedules")
}

func (c *Client) BatchDeleteSchedulesByNames(ctx context.Context, names []string) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	if len(names) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one schedule name is required", nil)
	}
	return c.batchDeleteSchedules(ctx, names, "")
}

func (c *Client) BatchDeleteSchedulesByDevice(ctx context.Context, deviceName string) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	if deviceName == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "device name is required", nil)
	}
	return c.batchDeleteSchedules(ctx, nil, deviceName)
}

func (c *Client) batchReadDevices(ctx context.Context, names []string, pattern string) ([]*xrtmodels.DeviceInfo, errors.EdgeX) {
	request := xrtmodels.NewBatchReadDevicesRequest(names, pattern, clientName)

	var response xrtmodels.BatchDevicesResponse
	if err := c.sendXrtRequest(ctx, request.RequestId, request, &response); err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to read the devices", err)
	}
	return response.Result.Devices, nil
}

func (c *Client) batchReadSchedules(ctx context.Context, names []string, deviceName, pattern string) ([]*xrtmodels.Schedule, errors.EdgeX) {
	request := xrtmodels.NewBatchReadSchedulesRequest(names, deviceName, pattern, clientName)

	var response xrtmodels.BatchSchedulesResponse
	if err := c.sendXrtRequest(ctx, request.RequestId, request, &response); err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to read the schedules", err)
	}
	return response.Result.Schedules, nil
}

func (c *Client) batchDeleteSchedules(ctx context.Context, names []string, deviceName string) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	request := xrtmodels.NewBatchDeleteSchedulesRequest(names, deviceName, clientName)
	return c.sendBatchScheduleRequest(ctx, request, request.RequestId, "failed to delete the schedules")
}

func (c *Client) sendBatchDeviceRequest(ctx context.Context, request any, requestID, errMsg string) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	var response xrtmodels.BatchDeviceResultsResponse
	if err := c.sendXrtRequest(ctx, requestID, request, &response); err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), errMsg, err)
	}

	results := make([]xrtmodels.BatchItemResult, 0, len(response.Result.Results))
	for _, entry := range response.Result.Results {
		results = append(results, xrtmodels.NewBatchItemResult(entry.Device, entry.BaseResult))
	}
	return results, nil
}

func (c *Client) sendBatchScheduleRequest(ctx context.Context, request any, requestID, errMsg string) ([]xrtmodels.BatchItemResult, errors.EdgeX) {
	var response xrtmodels.BatchScheduleResultsResponse
	if err := c.sendXrtRequest(ctx, requestID, request, &response); err != nil {
		return nil, errors.NewCommonEdgeX(errors.Kind(err), errMsg, err)
	}

	results := make([]xrtmodels.BatchItemResult, 0, len(response.Result.Results))
	for _, entry := range response.Result.Results {
		results = append(results, xrtmodels.NewBatchItemResult(entry.Schedule, entry.BaseResult))
	}
	return results, nil
}
