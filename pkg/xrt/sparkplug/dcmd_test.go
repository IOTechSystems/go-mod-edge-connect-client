// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"cmp"
	"context"
	goerrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging/mocks"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

const (
	bacnetDevice = "bacnet-sim-1"
	bacnetDCMD   = "spBv1.0/iotech/DCMD/xrt-bacnet-v4/bacnet-sim-1"
)

// The bacnet-sim-1 DBIRTH and DACKs were captured from XRT; its present-value resources are RW Float,
// event-state is R, and Device Control/Rebirth is W.
func newBacnetClient(t *testing.T, bus *mocks.MessageClient) *Client {
	t.Helper()
	c := newTestClient(bus)
	c.requestTimeout.Store(int64(time.Second))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-bacnet-v4", fixture(t, "nbirth_xrt-bacnet.json")))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-bacnet-v4/bacnet-sim-1", fixture(t, "dbirth_xrt-bacnet_bacnet-sim-1.json")))
	return c
}

// replyWithDACK answers the next DCMD with a DACK fixture carrying its CommandId, handled before PublishBinaryData
// returns (see request).
func replyWithDACK(t *testing.T, c *Client, bus *mocks.MessageClient, dackFixture string) *protobuf.Payload {
	t.Helper()
	var dcmd protobuf.Payload
	bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Run(func(args mock.Arguments) {
		require.NoError(t, proto.Unmarshal(args.Get(0).([]byte), &dcmd))
		dack := fixturePayload(t, dackFixture)
		dack.GetMetrics()[0].Value = &protobuf.Payload_Metric_StringValue{StringValue: commandID(&dcmd)}
		c.handle(message("spBv1.0/iotech/DACK/xrt-bacnet-v4/bacnet-sim-1", mustMarshal(t, dack)))
	}).Return(nil).Once()
	return &dcmd
}

// assertDCMDHeader checks the CommandId and AckGood metrics and returns the resource metrics after them.
func assertDCMDHeader(t *testing.T, dcmd *protobuf.Payload) []*protobuf.Payload_Metric {
	t.Helper()
	require.Greater(t, len(dcmd.GetMetrics()), 2)
	assert.NotZero(t, dcmd.GetTimestamp())
	assert.Nil(t, dcmd.Seq, "a DCMD carries no seq")
	id, ack := dcmd.GetMetrics()[0], dcmd.GetMetrics()[1]
	assert.Equal(t, "Device Control/CommandId", id.GetName())
	assert.Equal(t, uint32(protobuf.DataType_UUID), id.GetDatatype())
	_, err := uuid.Parse(id.GetStringValue())
	assert.NoError(t, err)
	assert.Equal(t, "Device Control/AckGood", ack.GetName())
	assert.Equal(t, uint32(protobuf.DataType_Boolean), ack.GetDatatype())
	assert.True(t, ack.GetBooleanValue())
	return dcmd.GetMetrics()[2:]
}

func assertNoDACKWaiters(t *testing.T, c *Client) {
	t.Helper()
	c.dackWaitersMu.Lock()
	defer c.dackWaitersMu.Unlock()
	assert.Empty(t, c.dackWaiters)
}

func TestReadDeviceResources(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	c := newBacnetClient(t, bus)
	dcmd := replyWithDACK(t, c, bus, "dack_read_bacnet-sim-1.json")

	names := []string{"analog_value_0:present-value", "analog_value_1:present-value", "analog_output_1:present-value"}
	result, err := c.ReadDeviceResources(context.Background(), bacnetNode, bacnetDevice, names)
	require.NoError(t, err)

	metrics := assertDCMDHeader(t, dcmd)
	require.Len(t, metrics, len(names))
	for i, m := range metrics {
		assert.Equal(t, names[i], m.GetName())
		assert.Equal(t, uint32(protobuf.DataType_Float), m.GetDatatype())
		assert.True(t, m.GetIsNull(), "XRT reads a metric only if is_null is true")
		assert.Nil(t, m.GetValue())
	}
	// The DACK lists the resources in reverse order.
	origin := int64(1792477039318) * int64(time.Millisecond)
	assert.Equal(t, xrtmodels.MultiResourcesResult{Device: bacnetDevice, Profile: "SimpleServer-5", Readings: map[string]xrtmodels.Reading{
		"analog_value_0:present-value":  {Value: float64(162), Type: common.ValueTypeFloat32, Origin: origin},
		"analog_value_1:present-value":  {Value: float64(61), Type: common.ValueTypeFloat32, Origin: origin},
		"analog_output_1:present-value": {Value: 66.5, Type: common.ValueTypeFloat32, Origin: origin},
	}}, result)
	assertNoDACKWaiters(t, c)
}

func TestReadingsConvertToEdgeXEvent(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	c := newBacnetClient(t, bus)
	replyWithDACK(t, c, bus, "dack_read_bacnet-sim-1.json")

	result, err := c.ReadDeviceResources(context.Background(), bacnetNode, bacnetDevice, []string{"analog_output_1:present-value"})
	require.NoError(t, err)
	event, err := xrtmodels.ToEdgeXV2EventDTO(result)
	require.NoError(t, err)
	require.Len(t, event.Readings, 1)
	assert.Equal(t, "SimpleServer-5", event.ProfileName)
	assert.Equal(t, common.ValueTypeFloat32, event.Readings[0].ValueType)
	assert.Equal(t, "6.65e+01", event.Readings[0].Value)
}

func TestWriteDeviceResources(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	c := newBacnetClient(t, bus)
	dcmd := replyWithDACK(t, c, bus, "dack_write_bacnet-sim-1.json")

	err := c.WriteDeviceResources(context.Background(), bacnetNode, bacnetDevice, map[string]any{
		"analog_value_0:present-value":  "11.5",
		"analog_value_1:present-value":  22.5,
		"analog_output_1:present-value": float32(33.5),
	}, nil)
	require.NoError(t, err)

	metrics := assertDCMDHeader(t, dcmd)
	require.Len(t, metrics, 3)
	want := map[string]float32{"analog_output_1:present-value": 33.5, "analog_value_0:present-value": 11.5, "analog_value_1:present-value": 22.5}
	for _, m := range metrics {
		assert.Equal(t, uint32(protobuf.DataType_Float), m.GetDatatype())
		assert.False(t, m.GetIsNull(), "XRT treats a metric with is_null true as a read")
		assert.Equal(t, want[m.GetName()], m.GetFloatValue(), m.GetName())
		delete(want, m.GetName())
	}
	assert.Empty(t, want)
	assertNoDACKWaiters(t, c)
}

func TestDCMDRejectedBeforePublish(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		kind errors.ErrKind
		call func(c *Client) errors.EdgeX
	}{
		{"offline node", errors.KindEntityDoesNotExist, func(c *Client) errors.EdgeX {
			_, err := c.ReadDeviceResources(ctx, modbusNode, bacnetDevice, []string{"analog_value_0:present-value"})
			return err
		}},
		{"offline device", errors.KindEntityDoesNotExist, func(c *Client) errors.EdgeX {
			_, err := c.ReadDeviceResources(ctx, bacnetNode, "bacnet-sim-2", []string{"analog_value_0:present-value"})
			return err
		}},
		{"unknown resource among valid ones", errors.KindEntityDoesNotExist, func(c *Client) errors.EdgeX {
			_, err := c.ReadDeviceResources(ctx, bacnetNode, bacnetDevice, []string{"analog_value_0:present-value", "NoSuchResource"})
			return err
		}},
		{"read a write-only resource", errors.KindNotAllowed, func(c *Client) errors.EdgeX {
			_, err := c.ReadDeviceResources(ctx, bacnetNode, bacnetDevice, []string{"Device Control/Rebirth"})
			return err
		}},
		{"read a resource without readWrite", errors.KindNotAllowed, func(c *Client) errors.EdgeX {
			_, err := c.ReadDeviceResources(ctx, bacnetNode, bacnetDevice, []string{"Device Control/CommandId"})
			return err
		}},
		{"read nothing", errors.KindContractInvalid, func(c *Client) errors.EdgeX {
			_, err := c.ReadDeviceResources(ctx, bacnetNode, bacnetDevice, nil)
			return err
		}},
		{"write a read-only resource", errors.KindNotAllowed, func(c *Client) errors.EdgeX {
			return c.WriteDeviceResources(ctx, bacnetNode, bacnetDevice,
				map[string]any{"analog_output_1:present-value": 1.5, "analog_value_1:event-state": 3}, nil)
		}},
		{"write an invalid value", errors.KindContractInvalid, func(c *Client) errors.EdgeX {
			return c.WriteDeviceResources(ctx, bacnetNode, bacnetDevice, map[string]any{"analog_output_1:present-value": "abc"}, nil)
		}},
		// Device Control/Rebirth has no alias in the DBIRTH.
		{"write a resource without alias", errors.KindNotImplemented, func(c *Client) errors.EdgeX {
			return c.WriteDeviceResources(ctx, bacnetNode, bacnetDevice, map[string]any{"Device Control/Rebirth": true}, nil)
		}},
		{"write with options", errors.KindContractInvalid, func(c *Client) errors.EdgeX {
			return c.WriteDeviceResources(ctx, bacnetNode, bacnetDevice,
				map[string]any{"analog_output_1:present-value": 1.5}, map[string]any{"opt": 1})
		}},
		{"write nothing", errors.KindContractInvalid, func(c *Client) errors.EdgeX {
			return c.WriteDeviceResources(ctx, bacnetNode, bacnetDevice, map[string]any{}, nil)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newBacnetClient(t, mocks.NewMessageClient(t)) // a publish would fail the mock
			err := tt.call(c)
			require.Error(t, err)
			assert.Equal(t, tt.kind, errors.Kind(err), err.Error())
		})
	}
}

func TestUnsupportedDatatypeRejectedBeforePublish(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.nodes[bacnetNode] = models.NodeInfo{NodeKey: bacnetNode, Devices: []models.Device{{Name: bacnetDevice, Metrics: []models.MetricDef{
		{Name: "table", Alias: 1, Datatype: uint32(protobuf.DataType_DataSet), ReadWrite: "R"},
	}}}}
	_, err := c.ReadDeviceResources(context.Background(), bacnetNode, bacnetDevice, []string{"table"})
	assert.Equal(t, errors.KindNotImplemented, errors.Kind(err))
}

func TestDCMDWithoutDACK(t *testing.T) {
	read := func(ctx context.Context, c *Client) errors.EdgeX {
		_, err := c.ReadDeviceResources(ctx, bacnetNode, bacnetDevice, []string{"analog_value_0:present-value"})
		return err
	}
	t.Run("timeout", func(t *testing.T) {
		bus := mocks.NewMessageClient(t)
		bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Return(nil).Once()
		c := newBacnetClient(t, bus)
		c.SetRequestTimeout(10 * time.Millisecond)
		assert.Equal(t, errors.KindServiceUnavailable, errors.Kind(read(context.Background(), c)))
		assertNoDACKWaiters(t, c)
	})
	t.Run("cancelled", func(t *testing.T) {
		bus := mocks.NewMessageClient(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Run(func(mock.Arguments) { cancel() }).Return(nil).Once()
		c := newBacnetClient(t, bus)
		err := read(ctx, c)
		assert.Equal(t, errors.KindServiceUnavailable, errors.Kind(err))
		assert.True(t, goerrors.Is(err, context.Canceled))
		assertNoDACKWaiters(t, c)
	})
	t.Run("client closed while waiting", func(t *testing.T) {
		bus := mocks.NewMessageClient(t)
		c := newBacnetClient(t, bus)
		c.SetRequestTimeout(time.Hour)
		c.done = make(chan struct{})
		bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Run(func(mock.Arguments) { close(c.done) }).Return(nil).Once()
		result := make(chan errors.EdgeX, 1)
		go func() { result <- read(context.Background(), c) }()
		select {
		case err := <-result:
			assert.Equal(t, errors.KindServiceUnavailable, errors.Kind(err))
		case <-time.After(time.Second):
			t.Fatal("a closed client waits for a DACK that cannot arrive")
		}
		assertNoDACKWaiters(t, c)
	})
	t.Run("publish failed", func(t *testing.T) {
		bus := mocks.NewMessageClient(t)
		bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Return(assert.AnError).Once()
		c := newBacnetClient(t, bus)
		assert.Equal(t, errors.KindCommunicationError, errors.Kind(read(context.Background(), c)))
		assertNoDACKWaiters(t, c)
	})
}

func TestUnanswerableDCMDIsNotSent(t *testing.T) {
	write := func(ctx context.Context, c *Client) errors.EdgeX {
		return c.WriteDeviceResources(ctx, bacnetNode, bacnetDevice, map[string]any{"analog_output_1:present-value": 1.5}, nil)
	}
	t.Run("client closed", func(t *testing.T) {
		c := newBacnetClient(t, mocks.NewMessageClient(t)) // a publish would fail the mock
		c.done = make(chan struct{})
		close(c.done)
		assert.Equal(t, errors.KindServiceUnavailable, errors.Kind(write(context.Background(), c)))
	})
	t.Run("cancelled", func(t *testing.T) {
		c := newBacnetClient(t, mocks.NewMessageClient(t))
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		cancel()
		err := write(ctx, c)
		assert.Equal(t, errors.KindServiceUnavailable, errors.Kind(err))
		assert.True(t, goerrors.Is(err, context.Canceled))
	})
	t.Run("timeout not positive", func(t *testing.T) {
		c := newBacnetClient(t, mocks.NewMessageClient(t))
		c.SetRequestTimeout(0)
		assert.Equal(t, errors.KindContractInvalid, errors.Kind(write(context.Background(), c)))
	})
}

// The other tests answer inside PublishBinaryData; here the DACK comes through run while the request waits.
func TestDACKDeliveredByRun(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	c := newBacnetClient(t, bus)
	c.done = make(chan struct{})
	messages := make(chan types.MessageEnvelope, 1)
	defer func() { <-c.done }() // deferred first, so it waits after cancel below stops run
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.run(ctx, messages, make(chan error))
	bus.On("Unsubscribe").Return(nil).Once()

	dack := fixturePayload(t, "dack_read_bacnet-sim-1.json")
	bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Run(func(args mock.Arguments) {
		var dcmd protobuf.Payload
		assert.NoError(t, proto.Unmarshal(args.Get(0).([]byte), &dcmd))
		dack.GetMetrics()[0].Value = &protobuf.Payload_Metric_StringValue{StringValue: commandID(&dcmd)}
		data, err := proto.Marshal(dack)
		assert.NoError(t, err)
		go func() {
			time.Sleep(20 * time.Millisecond) // let the request reach its select
			messages <- message("spBv1.0/iotech/DACK/xrt-bacnet-v4/bacnet-sim-1", data)
		}()
	}).Return(nil).Once()

	result, err := c.ReadDeviceResources(context.Background(), bacnetNode, bacnetDevice, []string{"analog_output_1:present-value"})
	require.NoError(t, err)
	assert.Equal(t, 66.5, result.Readings["analog_output_1:present-value"].Value)
	assertNoDACKWaiters(t, c)
}

func TestDuplicateDACKDoesNotBlock(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	ack := c.addDACKWaiter("id-1")
	dack := mustMarshal(t, newDCMD("id-1", nil))

	done := make(chan struct{})
	go func() {
		c.handle(message("spBv1.0/iotech/DACK/xrt-bacnet-v4/bacnet-sim-1", dack))
		c.handle(message("spBv1.0/iotech/DACK/xrt-bacnet-v4/bacnet-sim-1", dack))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("a second DACK with the same CommandId blocked message handling")
	}
	assert.Equal(t, "id-1", commandID(<-ack))
	assertNoDACKWaiters(t, c)
}

func bacnetDefs(t *testing.T, names ...string) []models.MetricDef {
	t.Helper()
	all := metricDefs(fixturePayload(t, "dbirth_xrt-bacnet_bacnet-sim-1.json"))
	defs := make([]models.MetricDef, 0, len(names))
	for _, name := range names {
		for _, def := range all {
			if def.Name == name {
				defs = append(defs, def)
			}
		}
	}
	require.Len(t, defs, len(names))
	return defs
}

// recordingLogger keeps the Errorf lines, which carry the quality codes that the error leaves out.
type recordingLogger struct {
	logger.LoggingClient
	mu    sync.Mutex
	lines []string
}

func (l *recordingLogger) Errorf(msg string, args ...any) { l.record("ERROR "+msg, args) }

func (l *recordingLogger) record(msg string, args []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(msg, args...))
}

func newRecordingClient(t *testing.T) (*Client, *recordingLogger) {
	t.Helper()
	lc := &recordingLogger{LoggingClient: logger.NewMockClient()}
	c := newTestClient(mocks.NewMessageClient(t))
	c.lc = lc
	return c, lc
}

// withQuality returns the write DACK fixture with the quality property set on every resource metric.
func withQuality(t *testing.T, q uint32) *protobuf.Payload {
	t.Helper()
	dack := fixturePayload(t, "dack_write_bacnet-sim-1.json")
	for _, m := range dack.GetMetrics()[1:] {
		m.Properties = &protobuf.Payload_PropertySet{
			Keys:   []string{"quality"},
			Values: []*protobuf.Payload_PropertyValue{{Value: &protobuf.Payload_PropertyValue_IntValue{IntValue: q}}},
		}
	}
	return dack
}

func TestAckedMetricsFailures(t *testing.T) {
	staleDef := bacnetDefs(t, "analog_value_0:present-value")[0]
	staleDef.Alias = bacnetDefs(t, "analog_value_1:present-value")[0].Alias
	textDef := bacnetDefs(t, "analog_value_0:present-value")[0]
	textDef.Datatype = uint32(protobuf.DataType_Text)
	present := bacnetDefs(t, "analog_value_0:present-value")
	tests := []struct {
		name    string
		dack    *protobuf.Payload
		defs    []models.MetricDef
		message string
		kind    errors.ErrKind // empty for KindServerError
		write   bool
	}{
		{"write rejected as read-only", fixturePayload(t, "dack_write_readonly_bacnet-sim-1.json"), bacnetDefs(t, "analog_value_1:event-state"),
			"analog_value_1:event-state: Write failed, the device or resource configuration does not allow this command (Bad_ConfigurationError, quality 0x80890000); see the XRT log for the cause", errors.KindNotAllowed, true},
		{"Bad_OutOfRange", withQuality(t, 0x803C0000), present,
			"analog_value_0:present-value: Read failed, the value is out of range (Bad_OutOfRange, quality 0x803C0000); see the XRT log for the cause", errors.KindContractInvalid, false},
		{"Bad_TypeMismatch on two resources", withQuality(t, 0x80740000), bacnetDefs(t, "analog_value_0:present-value", "analog_value_1:present-value"),
			"analog_value_0:present-value: Read failed, the value type does not match the resource (Bad_TypeMismatch, quality 0x80740000); see the XRT log for the cause; " +
				"analog_value_1:present-value: Read failed, the value type does not match the resource (Bad_TypeMismatch, quality 0x80740000); see the XRT log for the cause", errors.KindContractInvalid, false},
		{"Bad_OutOfRange with a resource left out", withQuality(t, 0x803C0000), bacnetDefs(t, "analog_value_0:present-value", "analog_value_1:event-state"),
			"analog_value_0:present-value: Read failed, the value is out of range (Bad_OutOfRange, quality 0x803C0000); see the XRT log for the cause; " +
				"analog_value_1:event-state: Read failed (missing from the DACK)", "", false},
		{"read without a value", fixturePayload(t, "dack_write_mismatch_bacnet-sim-1.json"), present,
			"analog_value_0:present-value: Read failed (no value of the DBIRTH datatype Float)", "", false},
		{"resource left out of the DACK", fixturePayload(t, "dack_read_bacnet-sim-1.json"), bacnetDefs(t, "analog_value_0:present-value", "analog_value_1:event-state"),
			"analog_value_1:event-state: Read failed (missing from the DACK)", "", false},
		{"stale alias", fixturePayload(t, "dack_read_bacnet-sim-1.json"), []models.MetricDef{staleDef},
			"analog_value_0:present-value: Read failed (its alias belongs to analog_value_1:present-value in the DACK; the DBIRTH may be stale)", "", false},
		{"value not of the DBIRTH datatype", fixturePayload(t, "dack_read_bacnet-sim-1.json"), []models.MetricDef{textDef},
			"analog_value_0:present-value: Read failed (no value of the DBIRTH datatype Text)", "", false},
		{"Bad_DependentValueChanged on two resources", withQuality(t, 0x80E30000), bacnetDefs(t, "analog_value_1:present-value", "analog_value_0:present-value"),
			"analog_value_1:present-value: Write failed, not written because another value in the same command was invalid (Bad_DependentValueChanged, quality 0x80E30000); see the XRT log for the cause; " +
				"analog_value_0:present-value: Write failed, not written because another value in the same command was invalid (Bad_DependentValueChanged, quality 0x80E30000); see the XRT log for the cause", "", true},
		{"Bad_NoCommunication", withQuality(t, 0x80310000), present,
			"analog_value_0:present-value: Read failed, unable to communicate with the device (Bad_NoCommunication, quality 0x80310000); see the XRT log for the cause", "", false},
		{"Bad quality with is_null", withNullValues(withQuality(t, 0x80310000)), present,
			"analog_value_0:present-value: Read failed, unable to communicate with the device (Bad_NoCommunication, quality 0x80310000); see the XRT log for the cause", "", false},
		{"Uncertain_LastUsableValue", withQuality(t, 0x40900000), present,
			"analog_value_0:present-value: Read failed, the device returned only its last usable value (Uncertain_LastUsableValue, quality 0x40900000); see the XRT log for the cause", "", false},
		// devsdk DEVSDK_QUALITY_BAD has no SubCode, so it has no specific message.
		{"Bad severity only", withQuality(t, 0x80000000), present,
			"analog_value_0:present-value: Read failed (quality 0x80000000); see the XRT log for the cause", "", false},
		{"quality code without a message", withQuality(t, 0x803B0000), present,
			"analog_value_0:present-value: Read failed (quality 0x803B0000); see the XRT log for the cause", "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, lc := newRecordingClient(t)
			acked, err := c.ackedMetrics(bacnetDCMD, tt.dack, tt.defs, !tt.write)
			require.Error(t, err)
			assert.Nil(t, acked)
			assert.Equal(t, cmp.Or(tt.kind, errors.KindServerError), errors.Kind(err))
			assert.Equal(t, tt.message, err.Error(), "the UI shows this message as is")
			assert.Equal(t, []string{"ERROR Sparkplug DCMD to " + bacnetDCMD + " failed: " + tt.message}, lc.lines)
		})
	}
}

func TestMergeKind(t *testing.T) {
	assert.Equal(t, errors.KindNotAllowed, mergeKind("", errors.KindNotAllowed))
	assert.Equal(t, errors.KindNotAllowed, mergeKind(errors.KindNotAllowed, errors.KindNotAllowed))
	assert.Equal(t, errors.KindServerError, mergeKind(errors.KindNotAllowed, errors.KindContractInvalid))
	assert.Equal(t, errors.KindServerError, mergeKind(errors.KindServerError, errors.KindContractInvalid))
}

func withNullValues(dack *protobuf.Payload) *protobuf.Payload {
	for _, m := range dack.GetMetrics()[1:] {
		m.Value, m.IsNull = nil, proto.Bool(true)
	}
	return dack
}

// Sparkplug lets a metric carry its name without its alias.
func TestAckedMetricsMatchesOnlyByAlias(t *testing.T) {
	dack := fixturePayload(t, "dack_read_bacnet-sim-1.json")
	for _, m := range dack.GetMetrics() {
		m.Alias = nil
	}
	c, lc := newRecordingClient(t)
	_, err := c.ackedMetrics(bacnetDCMD, dack, bacnetDefs(t, "analog_output_1:present-value"), true)
	require.Error(t, err)
	require.Len(t, lc.lines, 1)
	assert.Contains(t, lc.lines[0], "analog_output_1:present-value: Read failed (missing from the DACK)")
}

func TestAckedMetricsAcceptsGoodQuality(t *testing.T) {
	present := bacnetDefs(t, "analog_value_0:present-value")
	// 0x002D0000 is Good_SubscriptionTransferred: Good with a SubCode.
	for _, q := range []uint32{0, 0x002D0000} {
		c, lc := newRecordingClient(t)
		acked, err := c.ackedMetrics(bacnetDCMD, withQuality(t, q), present, true)
		require.NoError(t, err, "quality 0x%08X", q)
		assert.Equal(t, float32(11.5), acked["analog_value_0:present-value"].GetFloatValue())
		assert.Empty(t, lc.lines)
	}
}

func TestReadRejectsInexactInteger(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	c, lc := newRecordingClient(t)
	c.messageBus = bus
	c.requestTimeout.Store(int64(time.Second))
	c.nodes[bacnetNode] = models.NodeInfo{NodeKey: bacnetNode, Devices: []models.Device{{Name: bacnetDevice, Metrics: []models.MetricDef{
		{Name: "count", Alias: 1, Datatype: uint32(protobuf.DataType_UInt64), ReadWrite: "R"},
	}}}}
	bus.On("PublishBinaryData", mock.Anything, bacnetDCMD).Run(func(args mock.Arguments) {
		var dcmd protobuf.Payload
		require.NoError(t, proto.Unmarshal(args.Get(0).([]byte), &dcmd))
		dack := newDCMD(commandID(&dcmd), []*protobuf.Payload_Metric{
			{Name: proto.String("count"), Alias: proto.Uint64(1), Value: &protobuf.Payload_Metric_LongValue{LongValue: 1<<53 + 1}},
		})
		c.handle(message("spBv1.0/iotech/DACK/xrt-bacnet-v4/bacnet-sim-1", mustMarshal(t, dack)))
	}).Return(nil).Once()

	result, err := c.ReadDeviceResources(context.Background(), bacnetNode, bacnetDevice, []string{"count"})
	require.Error(t, err)
	assert.Empty(t, result.Readings)
	const want = "count: Read failed (UInt64 value 9007199254740993 is beyond 2^53)"
	assert.Equal(t, want, err.Error())
	assert.Equal(t, []string{"ERROR Sparkplug DCMD to " + bacnetDCMD + " failed: " + want}, lc.lines)
}

func TestDACKFailureReachesCaller(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	c := newBacnetClient(t, bus)
	replyWithDACK(t, c, bus, "dack_read_bacnet-sim-1.json") // has no analog_value_1:event-state

	_, err := c.ReadDeviceResources(context.Background(), bacnetNode, bacnetDevice, []string{"analog_value_1:event-state"})
	require.Error(t, err)
	assert.Equal(t, errors.KindServerError, errors.Kind(err))
	assert.Equal(t, "analog_value_1:event-state: Read failed (missing from the DACK)", err.Error())
	var edgexErr errors.CommonEdgeX
	require.True(t, goerrors.As(err, &edgexErr))
	assert.Equal(t, "analog_value_1:event-state: Read failed (missing from the DACK)", edgexErr.Message())
}

func TestMetricDefsContentType(t *testing.T) {
	defs := metricDefs(&protobuf.Payload{Metrics: []*protobuf.Payload_Metric{
		{Name: proto.String("obj"), Metadata: &protobuf.Payload_MetaData{ContentType: proto.String("xrt/json-object")}},
		{Name: proto.String("text")},
	}})
	assert.Equal(t, "xrt/json-object", defs[0].ContentType)
	assert.Empty(t, defs[1].ContentType)
}

func TestBinaryIsReadOnly(t *testing.T) {
	defs := []models.MetricDef{{Name: "bin", Alias: 1, Datatype: uint32(protobuf.DataType_Bytes), ReadWrite: "RW"}}
	_, err := resourceMetricDef(defs, "dev", "bin", common.ReadWrite_R)
	require.NoError(t, err)
	_, err = resourceMetricDef(defs, "dev", "bin", common.ReadWrite_W)
	assert.Equal(t, errors.KindNotImplemented, errors.Kind(err))
}

func TestObjectAckWithoutJSON(t *testing.T) {
	m := &protobuf.Payload_Metric{Name: proto.String("obj"), Alias: proto.Uint64(1), Value: &protobuf.Payload_Metric_StringValue{StringValue: "abc"}}
	assert.EqualError(t, validateResourceAck(objectDef, m, true), "obj: Read failed (the value of an Object is not JSON)")
	m.Value = &protobuf.Payload_Metric_StringValue{StringValue: `{"a":1}`}
	assert.NoError(t, validateResourceAck(objectDef, m, true))
}

// dack_write_mismatch is the DACK of a String written to a Float (see validateResourceAck).
func TestWriteAckJudgedByQualityOnly(t *testing.T) {
	present := bacnetDefs(t, "analog_value_0:present-value")
	c, lc := newRecordingClient(t)
	_, err := c.ackedMetrics(bacnetDCMD, fixturePayload(t, "dack_write_mismatch_bacnet-sim-1.json"), present, false)
	assert.NoError(t, err)
	assert.Empty(t, lc.lines)

	_, err = c.ackedMetrics(bacnetDCMD, withQuality(t, 0x803C0000), present, false)
	assert.Equal(t, errors.KindContractInvalid, errors.Kind(err))
	_, err = c.ackedMetrics(bacnetDCMD, fixturePayload(t, "dack_read_bacnet-sim-1.json"), bacnetDefs(t, "analog_value_1:event-state"), false)
	assert.Equal(t, errors.KindServerError, errors.Kind(err), "a resource left out of the DACK still fails a write")
}

func TestNullObject(t *testing.T) {
	m := &protobuf.Payload_Metric{Name: proto.String("obj"), Alias: proto.Uint64(1), Datatype: proto.Uint32(14), IsNull: proto.Bool(true)}
	assert.NoError(t, validateResourceAck(objectDef, m, true))
	m.IsNull = nil
	assert.NoError(t, validateResourceAck(objectDef, m, true), "a null Object is known by its missing value, not by is_null")
	reading, err := newReading(objectDef, m, &protobuf.Payload{Timestamp: proto.Uint64(1)})
	require.NoError(t, err)
	assert.Equal(t, xrtmodels.Reading{Value: nil, Type: common.ValueTypeObject, Origin: 1_000_000}, reading)

	textDef := objectDef
	textDef.ContentType = ""
	assert.EqualError(t, validateResourceAck(textDef, m, true), "obj: Read failed (no value of the DBIRTH datatype Text)", "only an Object reads null")
}
