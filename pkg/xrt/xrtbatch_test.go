// Copyright (C) 2026 IOTech Ltd

package xrt

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"reflect"
	"testing"
	"time"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/dtos"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// The reply shapes here are captured verbatim from XRT 3.4.6.
func TestBatchResponseDecoding(t *testing.T) {
	t.Run("a missing device decodes as nil", assertMissingDeviceDecodesAsNil)
	t.Run("a failed item decodes under a successful envelope", assertFailedItemUnderSuccessfulEnvelope)
	t.Run("a schedule result decodes from the schedule field", assertScheduleResultNamedFromScheduleField)
}

// A missing device comes back as null, which must stay distinguishable from a device whose
// fields happen to be zero.
func assertMissingDeviceDecodesAsNil(t *testing.T) {
	const reply = `{"client":"probe","request_id":"r1","result":{"devices":[` +
		`{"name":"modbus-sim","operational":true,"profileName":"modbus-sim-profile"},` +
		`null],"status":0},"type":"xrt.reply:1.0"}`

	var response xrtmodels.BatchDevicesResponse
	if err := json.Unmarshal([]byte(reply), &response); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(response.Result.Devices) != 2 {
		t.Fatalf("got %d devices, want 2", len(response.Result.Devices))
	}
	if response.Result.Devices[0] == nil || response.Result.Devices[0].Name != "modbus-sim" {
		t.Errorf("first device: got %v, want modbus-sim", response.Result.Devices[0])
	}
	if response.Result.Devices[1] != nil {
		t.Errorf("second device: got %v, want nil", response.Result.Devices[1])
	}
}

// XRT reports per-item failures while the envelope status stays 0, so the outer status must
// not be treated as the outcome of every item.
func assertFailedItemUnderSuccessfulEnvelope(t *testing.T) {
	const reply = `{"client":"probe","request_id":"a1","result":{"device_results":[` +
		`{"device":"batch-dev-1","status":0},` +
		`{"device":"batch-dev-2","error":"could not load profile","status":10}` +
		`],"status":0},"type":"xrt.reply:1.0"}`

	var response xrtmodels.BatchDeviceResultsResponse
	if err := json.Unmarshal([]byte(reply), &response); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response.Result.Status != 0 {
		t.Errorf("envelope status: got %d, want 0", response.Result.Status)
	}
	if len(response.Result.Results) != 2 {
		t.Fatalf("got %d entries, want 2", len(response.Result.Results))
	}

	first := xrtmodels.NewBatchItemResult(response.Result.Results[0].Device,
		response.Result.Results[0].BaseResult)
	if first.Name != "batch-dev-1" || first.Failed() || first.Err != nil {
		t.Errorf("first result: got %+v, want a success for batch-dev-1", first)
	}

	second := xrtmodels.NewBatchItemResult(response.Result.Results[1].Device,
		response.Result.Results[1].BaseResult)
	if !second.Failed() || second.Err == nil {
		t.Errorf("second result: got %+v, want a failure carrying an error", second)
	}
}

// A schedule reply names its items with a different field, which is why the two result
// shapes are separate types.
func assertScheduleResultNamedFromScheduleField(t *testing.T) {
	const reply = `{"result":{"schedule_results":[` +
		`{"schedule":"s1","status":0},` +
		`{"schedule":"s2","error":"error_message","status":4}` +
		`],"status":0}}`

	var response xrtmodels.BatchScheduleResultsResponse
	if err := json.Unmarshal([]byte(reply), &response); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(response.Result.Results) != 2 {
		t.Fatalf("got %d entries, want 2", len(response.Result.Results))
	}
	if response.Result.Results[0].Schedule != "s1" {
		t.Errorf("got %q, want s1", response.Result.Results[0].Schedule)
	}
	failed := xrtmodels.NewBatchItemResult(response.Result.Results[1].Schedule,
		response.Result.Results[1].BaseResult)
	if failed.Name != "s2" || !failed.Failed() {
		t.Errorf("got %+v, want a failure for s2", failed)
	}
}

// A read with no argument must omit the selector fields rather than send them empty,
// because XRT treats an empty request as "all".
// The want strings are the exact request bodies accepted by XRT 3.4.6, captured from a
// live exchange. Unset selector fields must be absent rather than empty, because XRT
// reads a request with no selector as "everything".
func TestBatchRequestEncoding(t *testing.T) {
	tests := []struct {
		name    string
		request any
		want    string
	}{
		{"read devices, no selector", xrtmodels.NewBatchReadDevicesRequest(nil, "", clientName),
			`{"op":"device:read_batch"}`},
		{"read devices by names", xrtmodels.NewBatchReadDevicesRequest([]string{"dev1", "dev2"}, "", clientName),
			`{"op":"device:read_batch","devices":["dev1","dev2"]}`},
		{"read devices by pattern", xrtmodels.NewBatchReadDevicesRequest(nil, "mod.*", clientName),
			`{"op":"device:read_batch","pattern":"mod.*"}`},
		{"delete devices", xrtmodels.NewBatchDeleteDevicesRequest([]string{"dev1", "dev2"}, clientName),
			`{"op":"device:delete_batch","devices":["dev1","dev2"]}`},

		{"read schedules, no selector", xrtmodels.NewBatchReadSchedulesRequest(nil, "", "", clientName),
			`{"op":"schedule:read_batch"}`},
		{"read schedules by names", xrtmodels.NewBatchReadSchedulesRequest([]string{"s1"}, "", "", clientName),
			`{"op":"schedule:read_batch","schedules":["s1"]}`},
		{"read schedules by device", xrtmodels.NewBatchReadSchedulesRequest(nil, "dev1", "", clientName),
			`{"op":"schedule:read_batch","device":"dev1"}`},
		{"read schedules by pattern", xrtmodels.NewBatchReadSchedulesRequest(nil, "", ".*", clientName),
			`{"op":"schedule:read_batch","pattern":".*"}`},
		{"delete schedules by names", xrtmodels.NewBatchDeleteSchedulesRequest([]string{"s1"}, "", clientName),
			`{"op":"schedule:delete_batch","schedules":["s1"]}`},
		{"delete schedules by device", xrtmodels.NewBatchDeleteSchedulesRequest(nil, "dev1", clientName),
			`{"op":"schedule:delete_batch","device":"dev1"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assertRequestBody(t, tc.request, tc.want)
		})
	}
}

// XRT expects each added device as a name/info pair, with the device nested under
// device_info; this is the shape a single device:add sends, repeated.
func TestBatchAddDevicesRequestEncoding(t *testing.T) {
	device, err := xrtmodels.ToXrtDevice(dtos.Device{
		Name:        "dev1",
		ProfileName: "test_profile",
		Protocols:   map[string]dtos.ProtocolProperties{"Other": {"Address": "device-virtual-01"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	request := xrtmodels.NewBatchAddDevicesRequest([]xrtmodels.DeviceInfo{device}, clientName)

	encoded, marshalErr := json.Marshal(request)
	if marshalErr != nil {
		t.Fatalf("unexpected error: %v", marshalErr)
	}
	var decoded struct {
		Op      string `json:"op"`
		Devices []struct {
			Device     string `json:"device"`
			DeviceInfo struct {
				Name        string `json:"name"`
				ProfileName string `json:"profileName"`
			} `json:"device_info"`
		} `json:"devices"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if decoded.Op != xrtmodels.BatchAddDevicesOperation {
		t.Errorf("op: got %q, want %q", decoded.Op, xrtmodels.BatchAddDevicesOperation)
	}
	if len(decoded.Devices) != 1 {
		t.Fatalf("got %d devices, want 1", len(decoded.Devices))
	}
	// The outer name is derived from the device, so the two can never disagree.
	if decoded.Devices[0].Device != "dev1" || decoded.Devices[0].DeviceInfo.Name != "dev1" {
		t.Errorf("names: got %q / %q, want dev1 / dev1",
			decoded.Devices[0].Device, decoded.Devices[0].DeviceInfo.Name)
	}
	if decoded.Devices[0].DeviceInfo.ProfileName != "test_profile" {
		t.Errorf("profile: got %q, want test_profile", decoded.Devices[0].DeviceInfo.ProfileName)
	}
}

// assertRequestBody compares the encoded request with want, ignoring client and
// request_id because they vary per call.
func assertRequestBody(t *testing.T, request any, want string) {
	t.Helper()

	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(encoded, &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["client"] != clientName {
		t.Errorf("client: got %v, want %q", got["client"], clientName)
	}
	if got["request_id"] == "" {
		t.Error("request_id must be set")
	}
	delete(got, "client")
	delete(got, "request_id")

	var wantMap map[string]any
	if err := json.Unmarshal([]byte(want), &wantMap); err != nil {
		t.Fatalf("the want string is not valid JSON: %v", err)
	}
	if !reflect.DeepEqual(got, wantMap) {
		gotJSON, _ := json.Marshal(got)
		t.Errorf("request body:\n  got  %s\n  want %s", gotJSON, want)
	}
}

// An empty argument is a mistake, not a request for everything — BatchReadAll* covers
// that. Asserting on publication rather than err != nil is what makes this meaningful:
// this Client has no reply topic manager, so every call fails either way.
func TestBatchMethodsRejectEmptyArguments(t *testing.T) {
	tests := []struct {
		name       string
		callEmpty  func(context.Context, interfaces.EdgeClient) error
		callFilled func(context.Context, interfaces.EdgeClient) error
	}{
		{"read devices by names",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadDevicesByNames(ctx, nil)
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadDevicesByNames(ctx, []string{"dev-1"})
				return err
			}},
		{"read devices by pattern",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadDevicesByPattern(ctx, "")
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadDevicesByPattern(ctx, ".*")
				return err
			}},
		{"add devices",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchAddDevices(ctx, nil)
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchAddDevices(ctx, []dtos.Device{{Name: "dev-1"}})
				return err
			}},
		{"delete devices",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchDeleteDevices(ctx, nil)
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchDeleteDevices(ctx, []string{"dev-1"})
				return err
			}},
		{"read schedules by names",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadSchedulesByNames(ctx, nil)
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadSchedulesByNames(ctx, []string{"s1"})
				return err
			}},
		{"read schedules by device",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadSchedulesByDevice(ctx, "")
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadSchedulesByDevice(ctx, "dev-1")
				return err
			}},
		{"read schedules by pattern",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadSchedulesByPattern(ctx, "")
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchReadSchedulesByPattern(ctx, ".*")
				return err
			}},
		{"add schedules",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchAddSchedules(ctx, nil)
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchAddSchedules(ctx, []xrtmodels.Schedule{{Name: "s1"}})
				return err
			}},
		// Delete always names its targets: the specification has no "delete all".
		{"delete schedules by names",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchDeleteSchedulesByNames(ctx, nil)
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchDeleteSchedulesByNames(ctx, []string{"s1"})
				return err
			}},
		{"delete schedules by device",
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchDeleteSchedulesByDevice(ctx, "")
				return err
			},
			func(ctx context.Context, c interfaces.EdgeClient) error {
				_, err := c.BatchDeleteSchedulesByDevice(ctx, "dev-1")
				return err
			}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			emptyClient, emptyBus := newPublishRecordingClient(t)
			if err := tc.callEmpty(t.Context(), emptyClient); err == nil {
				t.Fatal("expected an error for an empty argument")
			}
			if emptyBus.published {
				t.Error("an empty argument must be rejected before anything is published")
			}

			// The same method with a usable argument does publish, so the rejection above
			// came from the argument check.
			filledClient, filledBus := newPublishRecordingClient(t)
			_ = tc.callFilled(t.Context(), filledClient)
			if !filledBus.published {
				t.Error("a non-empty argument should have reached the message bus")
			}
		})
	}
}

// The conversion from the wire result entries to BatchItemResult is what callers act on,
// so it is exercised end to end against a bus that replies with a real XRT body.
func TestBatchResultConversion(t *testing.T) {
	tests := []struct {
		name  string
		reply string
		call  func(context.Context, interfaces.EdgeClient) ([]xrtmodels.BatchItemResult, error)
		want  []xrtmodels.BatchItemResult
	}{
		{
			name: "device results keep their order and per-item status",
			reply: `{"result":{"device_results":[` +
				`{"device":"dev-1","status":0},` +
				`{"device":"dev-2","error":"could not load profile","status":10},` +
				`{"device":"dev-3","status":0}` +
				`],"status":0}}`,
			call: func(ctx context.Context, c interfaces.EdgeClient) ([]xrtmodels.BatchItemResult, error) {
				return c.BatchDeleteDevices(ctx, []string{"dev-1", "dev-2", "dev-3"})
			},
			want: []xrtmodels.BatchItemResult{
				{Name: "dev-1", Status: 0},
				{Name: "dev-2", Status: 10},
				{Name: "dev-3", Status: 0},
			},
		},
		{
			name: "schedule results are named from the schedule field",
			reply: `{"result":{"schedule_results":[` +
				`{"schedule":"s1","status":0},` +
				`{"schedule":"s2","error":"error_message","status":4}` +
				`],"status":0}}`,
			call: func(ctx context.Context, c interfaces.EdgeClient) ([]xrtmodels.BatchItemResult, error) {
				return c.BatchDeleteSchedulesByNames(ctx, []string{"s1", "s2"})
			},
			want: []xrtmodels.BatchItemResult{
				{Name: "s1", Status: 0},
				{Name: "s2", Status: 4},
			},
		},
		{
			name:  "an empty result list is not an error",
			reply: `{"result":{"device_results":[],"status":0}}`,
			call: func(ctx context.Context, c interfaces.EdgeClient) ([]xrtmodels.BatchItemResult, error) {
				return c.BatchDeleteDevices(ctx, []string{"dev-1"})
			},
			want: []xrtmodels.BatchItemResult{},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newReplyingClient(t, tc.reply)

			got, err := tc.call(t.Context(), client)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertBatchItemResults(t, got, tc.want)
		})
	}
}

func assertBatchItemResults(t *testing.T, got, want []xrtmodels.BatchItemResult) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("got %d results, want %d: %+v", len(got), len(want), got)
	}
	for i, wanted := range want {
		if got[i].Name != wanted.Name {
			t.Errorf("result %d name: got %q, want %q", i, got[i].Name, wanted.Name)
		}
		if got[i].Status != wanted.Status {
			t.Errorf("result %d status: got %d, want %d", i, got[i].Status, wanted.Status)
		}
		if failed := wanted.Status != 0; got[i].Failed() != failed {
			t.Errorf("result %d Failed(): got %v, want %v", i, got[i].Failed(), failed)
		}
		if wanted.Status != 0 && got[i].Err == nil {
			t.Errorf("result %d: a failed item must carry an error", i)
		}
		if wanted.Status == 0 && got[i].Err != nil {
			t.Errorf("result %d: a successful item must not carry an error, got %v", i, got[i].Err)
		}
	}
}

// replyingBus answers every publish with reply, echoing back the request_id so the
// waiting caller is the one woken up.
type replyingBus struct {
	messaging.MessageClient
	reply    string
	messages chan types.MessageEnvelope
}

func (bus *replyingBus) PublishBinaryData(payload []byte, _ string) error {
	var request struct {
		RequestId string `json:"request_id"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	var body map[string]any
	if err := json.Unmarshal([]byte(bus.reply), &body); err != nil {
		return err
	}
	body["request_id"] = request.RequestId
	encoded, err := json.Marshal(body)
	if err != nil {
		return err
	}

	go func() {
		bus.messages <- types.MessageEnvelope{Payload: encoded, ContentType: common.ContentTypeJSON}
	}()
	return nil
}

func (bus *replyingBus) SubscribeBinaryData(topics []types.TopicChannel, _ chan error) error {
	bus.messages = topics[0].Messages
	return nil
}

func (bus *replyingBus) Unsubscribe(...string) error { return nil }

func newReplyingClient(t *testing.T, reply string) interfaces.EdgeClient {
	t.Helper()

	bus := &replyingBus{reply: reply}
	client, err := NewXrtClient(t.Context(), bus, "test/request", "test/reply/"+t.Name(), time.Second,
		logger.MockLogger{}, nil)
	if err != nil {
		t.Fatalf("failed to create the client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// publishRecordingBus notes whether anything was published. Publish fails so the call
// returns promptly instead of waiting for a reply that will never arrive.
type publishRecordingBus struct {
	messaging.MessageClient
	published bool
}

func (bus *publishRecordingBus) PublishBinaryData([]byte, string) error {
	bus.published = true
	return goerrors.New("publish disabled in this test")
}

func (bus *publishRecordingBus) SubscribeBinaryData([]types.TopicChannel, chan error) error {
	return nil
}

func (bus *publishRecordingBus) Unsubscribe(...string) error { return nil }

func newPublishRecordingClient(t *testing.T) (interfaces.EdgeClient, *publishRecordingBus) {
	t.Helper()

	bus := &publishRecordingBus{}
	// A unique reply topic per client keeps TmPool from sharing subscriptions between
	// subtests.
	replyTopic := "test/reply/" + t.Name()
	client, err := NewXrtClient(t.Context(), bus, "test/request", replyTopic, time.Millisecond,
		logger.MockLogger{}, nil)
	if err != nil {
		t.Fatalf("failed to create the client: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, bus
}
