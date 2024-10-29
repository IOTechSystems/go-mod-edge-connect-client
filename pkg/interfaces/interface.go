// Copyright (C) 2023-2024 IOTech Ltd

package interfaces

import (
	"context"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/xrtmodels"
)

// EdgeClient defines the interface for interactions with the Edge service.
type EdgeClient interface {
	AllDevices(ctx context.Context) ([]string, errors.EdgeX)
	DeviceByName(ctx context.Context, name string) (xrtmodels.DeviceInfo, errors.EdgeX)
	AddDevice(ctx context.Context, device dtos.Device) errors.EdgeX
	UpdateDevice(ctx context.Context, device dtos.Device) errors.EdgeX
	DeleteDeviceByName(ctx context.Context, name string) errors.EdgeX
	AddDiscoveredDevice(ctx context.Context, device dtos.Device) errors.EdgeX
	ScanDevice(ctx context.Context, device dtos.Device, options map[string]any) errors.EdgeX

	ReadDeviceResources(ctx context.Context, deviceName string, resourceNames []string) (xrtmodels.MultiResourcesResult, errors.EdgeX)
	WriteDeviceResources(ctx context.Context, deviceName string, resourceValuePairs, options map[string]any) errors.EdgeX

	AllSchedules(ctx context.Context) ([]string, errors.EdgeX)
	AddSchedule(ctx context.Context, schedule xrtmodels.Schedule) errors.EdgeX
	DeleteScheduleByName(ctx context.Context, scheduleName string) errors.EdgeX

	AllDeviceProfiles(ctx context.Context) ([]string, errors.EdgeX)
	DeviceProfileByName(ctx context.Context, name string) (dtos.DeviceProfile, errors.EdgeX)
	AddDeviceProfile(ctx context.Context, device dtos.DeviceProfile) errors.EdgeX
	UpdateDeviceProfile(ctx context.Context, device dtos.DeviceProfile) errors.EdgeX
	DeleteDeviceProfileByName(ctx context.Context, name string) errors.EdgeX

	UpdateLuaScript(ctx context.Context, luaScript string) errors.EdgeX
	DiscoverComponents(ctx context.Context, category string, subscribeTimeout time.Duration) ([]xrtmodels.MultiComponentsResponse, errors.EdgeX)

	TriggerDiscovery(ctx context.Context) errors.EdgeX

	// SetResponseTimeout sets responseTimeout to XrtClient
	SetResponseTimeout(responseTimeout time.Duration)

	// Close closes the connection of XRT client
	Close() errors.EdgeX
}
