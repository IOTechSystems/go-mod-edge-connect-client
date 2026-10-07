// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"context"
	"io/fs"
	"os"
	"testing"
	"time"

	"github.com/IOTechSystems/sparkplug-sdk-go/pkg/sparkplug/protobuf"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging/mocks"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
)

var (
	modbusNode = models.NodeKey{Group: "iotech", Node: "xrt-modbus-v3.4"}
	bacnetNode = models.NodeKey{Group: "iotech", Node: "xrt-bacnet-v3.4"}
)

// The testdata files are NBIRTH/DBIRTH captured from XRT 3.4.6 (protojson), e.g. modbus bdSeq 38, trimmed to the
// metrics the tests need; each Service/* config keeps only its Name.
func fixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := fs.ReadFile(os.DirFS("testdata"), name)
	require.NoError(t, err)
	var p protobuf.Payload
	require.NoError(t, protojson.Unmarshal(raw, &p))
	data, err := proto.Marshal(&p)
	require.NoError(t, err)
	return data
}

func deathPayload(t *testing.T, seq int64) []byte {
	t.Helper()
	name, datatype, value := "bdSeq", uint32(protobuf.DataType_Int64), uint64(seq)
	data, err := proto.Marshal(&protobuf.Payload{Metrics: []*protobuf.Payload_Metric{{
		Name: &name, Datatype: &datatype, Value: &protobuf.Payload_Metric_LongValue{LongValue: value},
	}}})
	require.NoError(t, err)
	return data
}

func message(topic string, data []byte) types.MessageEnvelope {
	return types.MessageEnvelope{ReceivedTopic: topic, Payload: data}
}

// newTestClient builds a Client without subscribing, so messages are fed straight to handle.
func newTestClient(bus *mocks.MessageClient) *Client {
	return &Client{
		lc:         logger.NewMockClient(),
		messageBus: bus,
		groups:     []string{"iotech"},
		cancel:     func() {},
		nodes:      make(map[models.NodeKey]models.NodeInfo),
		reborn:     make(map[models.NodeKey]struct{}),
	}
}

func onlyNode(t *testing.T, c *Client) models.NodeInfo {
	t.Helper()
	nodes := c.Nodes()
	require.Len(t, nodes, 1)
	return nodes[0]
}

func TestNewClient(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	var subscribed []types.TopicChannel
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { subscribed = args.Get(0).([]types.TopicChannel) }).
		Return(nil).Once()

	client, err := NewClient(context.Background(), bus, []string{"iotech", "site-b"}, time.Second, logger.NewMockClient())
	require.NoError(t, err)
	defer func() {
		bus.On("Unsubscribe", anyTopics(10)...).Return(nil).Once()
		require.NoError(t, client.Close())
	}()

	topics := make([]string, 0, len(subscribed))
	for _, tc := range subscribed {
		topics = append(topics, tc.Topic)
	}
	assert.ElementsMatch(t, []string{
		"spBv1.0/iotech/NBIRTH/+", "spBv1.0/iotech/NDEATH/+", "spBv1.0/iotech/DBIRTH/+/+", "spBv1.0/iotech/DDEATH/+/+",
		"spBv1.0/iotech/DACK/+/+",
		"spBv1.0/site-b/NBIRTH/+", "spBv1.0/site-b/NDEATH/+", "spBv1.0/site-b/DBIRTH/+/+", "spBv1.0/site-b/DDEATH/+/+",
		"spBv1.0/site-b/DACK/+/+",
	}, topics)

	// A message on the subscribed channel reaches the node state through the run goroutine.
	subscribed[0].Messages <- message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json"))
	assert.Eventually(t, func() bool { return len(client.Nodes()) == 1 }, time.Second, 10*time.Millisecond)
}

func TestNewClientWithoutGroups(t *testing.T) {
	_, err := NewClient(context.Background(), mocks.NewMessageClient(t), nil, time.Second, logger.NewMockClient())
	require.Error(t, err)
	assert.Equal(t, errors.KindContractInvalid, errors.Kind(err))
}

func TestBirth(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", fixture(t, "dbirth_xrt-modbus_modbus-sim.json")))

	node := onlyNode(t, c)
	assert.Equal(t, modbusNode, node.NodeKey)
	assert.Equal(t, int64(38), node.BdSeq)
	assert.Equal(t, []string{"bacnet_ip", "modbus"}, node.Services)
	assert.Contains(t, node.Metrics, models.MetricDef{Name: "bdSeq", Datatype: uint32(protobuf.DataType_Int64)})

	require.Len(t, node.Devices, 1)
	device := node.Devices[0]
	assert.Equal(t, "modbus-sim", device.Name)
	assert.Len(t, device.Metrics, 3)
	assert.Contains(t, device.Metrics, models.MetricDef{Name: "Voltage", Alias: 7916512088, Datatype: 6, ReadWrite: "RW"})
}

// A rebirth re-publishes NBIRTH, which must drop the devices of the earlier birth until their DBIRTHs come again.
func TestNBIRTHClearsDevices(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", fixture(t, "dbirth_xrt-modbus_modbus-sim.json")))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))

	assert.Empty(t, onlyNode(t, c).Devices)
}

func TestDevices(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	dbirth := fixture(t, "dbirth_xrt-modbus_modbus-sim.json")
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", dbirth))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/other", dbirth))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", dbirth)) // replaces, no duplicate

	deviceNames := func() []string {
		var names []string
		for _, d := range onlyNode(t, c).Devices {
			names = append(names, d.Name)
		}
		return names
	}
	assert.ElementsMatch(t, []string{"modbus-sim", "other"}, deviceNames())

	before := deviceNames()
	beforeDevices := onlyNode(t, c).Devices
	c.handle(message("spBv1.0/iotech/DDEATH/xrt-modbus-v3.4/other", nil))
	assert.Equal(t, []string{"modbus-sim"}, deviceNames())

	var stillThere []string
	for _, d := range beforeDevices {
		stillThere = append(stillThere, d.Name)
	}
	assert.Equal(t, before, stillThere, "a slice returned earlier must not change")
}

func TestNDEATH(t *testing.T) {
	tests := []struct {
		name    string
		stored  int64
		death   []byte
		removed bool
	}{
		{"same bdSeq", 38, deathPayload(t, 38), true},
		{"newer bdSeq: NBIRTH of a later session was missed", 38, deathPayload(t, 39), true},
		{"newer across the wrap", 255, deathPayload(t, 0), true},
		{"newest distance 127", 0, deathPayload(t, 127), true},
		{"older bdSeq: stale Will", 38, deathPayload(t, 37), false},
		{"distance 128 counts as older", 0, deathPayload(t, 128), false},
		{"no bdSeq", 38, mustMarshal(t, &protobuf.Payload{}), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(mocks.NewMessageClient(t))
			c.nodes[modbusNode] = models.NodeInfo{NodeKey: modbusNode, BdSeq: tt.stored}
			c.handle(message("spBv1.0/iotech/NDEATH/xrt-modbus-v3.4", tt.death))
			assert.Equal(t, tt.removed, len(c.Nodes()) == 0)
		})
	}
}

func mustMarshal(t *testing.T, p *protobuf.Payload) []byte {
	t.Helper()
	data, err := proto.Marshal(p)
	require.NoError(t, err)
	return data
}

// The per-node rebirth is published in the background, so tests observe it through this channel.
func expectNodeRebirth(bus *mocks.MessageClient, err error, times int) chan []byte {
	published := make(chan []byte, times)
	bus.On("PublishBinaryData", mock.Anything, "spBv1.0/iotech/NCMD/xrt-modbus-v3.4").
		Run(func(args mock.Arguments) { published <- args.Get(0).([]byte) }).
		Return(err).Times(times)
	return published
}

func waitPublished(t *testing.T, published chan []byte) []byte {
	t.Helper()
	select {
	case data := <-published:
		return data
	case <-time.After(time.Second):
		t.Fatal("expected a rebirth publish")
		return nil
	}
}

func assertNotPublished(t *testing.T, published chan []byte) {
	t.Helper()
	select {
	case <-published:
		t.Fatal("unexpected rebirth publish")
	case <-time.After(50 * time.Millisecond):
	}
}

func rebornPending(c *Client, key models.NodeKey) bool {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()
	_, ok := c.reborn[key]
	return ok
}

func TestUnknownNodeIsRebornOnce(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	published := expectNodeRebirth(bus, nil, 2)
	c := newTestClient(bus)

	dbirth := fixture(t, "dbirth_xrt-modbus_modbus-sim.json")
	for i := 0; i < 10; i++ {
		c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", dbirth))
		c.handle(message("spBv1.0/iotech/DDEATH/xrt-modbus-v3.4/modbus-sim", nil))
	}
	assertRebirth(t, waitPublished(t, published))
	assertNotPublished(t, published) // 20 unknown messages, one rebirth
	assert.Empty(t, c.Nodes())

	// The NBIRTH clears the mark, so a later unknown message asks again.
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/NDEATH/xrt-modbus-v3.4", deathPayload(t, 38)))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", dbirth))
	waitPublished(t, published)
}

func assertRebirth(t *testing.T, data []byte) {
	t.Helper()
	var p protobuf.Payload
	require.NoError(t, proto.Unmarshal(data, &p))
	require.Len(t, p.GetMetrics(), 1)
	assert.Equal(t, "Node Control/Rebirth", p.GetMetrics()[0].GetName())
	assert.True(t, p.GetMetrics()[0].GetBooleanValue())
}

func TestRebirth(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	bus.On("PublishBinaryData", mock.Anything, "spBv1.0/iotech/NCMD").Return(nil).Once()
	bus.On("PublishBinaryData", mock.Anything, "spBv1.0/site-b/NCMD").Return(assert.AnError).Once()
	c := newTestClient(bus)
	c.groups = []string{"iotech", "site-b"}

	err := c.Rebirth()
	require.Error(t, err, "a failed group is reported")
	assert.Equal(t, errors.KindCommunicationError, errors.Kind(err))
	assertRebirth(t, bus.Calls[0].Arguments.Get(0).([]byte))
}

func TestNodesSortedByGroupThenNode(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.handle(message("spBv1.0/site-b/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-bacnet-v3.4", fixture(t, "nbirth_xrt-bacnet.json")))

	var keys []models.NodeKey
	for _, n := range c.Nodes() {
		keys = append(keys, n.NodeKey)
	}
	assert.Equal(t, []models.NodeKey{bacnetNode, modbusNode, {Group: "site-b", Node: "xrt-modbus-v3.4"}}, keys)
}

func TestHandleSkipsInvalidMessages(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.handle(types.MessageEnvelope{ReceivedTopic: "spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", Payload: "not bytes"})
	c.handle(message("spBv1.0/iotech", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", []byte{0xff}))
	assert.Empty(t, c.Nodes())
}

func TestDCMDNotImplemented(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	_, err := c.ReadDeviceResources(context.Background(), modbusNode, "modbus-sim", []string{"Voltage"})
	assert.Equal(t, errors.KindNotImplemented, errors.Kind(err))
	err = c.WriteDeviceResources(context.Background(), modbusNode, "modbus-sim", map[string]any{"Voltage": 1}, nil)
	assert.Equal(t, errors.KindNotImplemented, errors.Kind(err))
}

func anyTopics(n int) []any {
	args := make([]any, n)
	for i := range args {
		args[i] = mock.Anything
	}
	return args
}

func TestNewClientSubscribeFailureUnsubscribes(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).Return(assert.AnError).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(nil).Maybe()

	_, err := NewClient(context.Background(), bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.Error(t, err)
	bus.AssertCalled(t, "Unsubscribe", anyTopics(5)...)
}

// The caller may cancel ctx without calling Close (see run).
func TestCtxCancelUnsubscribesWhileDraining(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	var channels []types.TopicChannel
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { channels = args.Get(0).([]types.TopicChannel) }).Return(nil).Once()
	release := make(chan struct{})
	unsubscribed := make(chan struct{})
	bus.On("Unsubscribe", anyTopics(5)...).
		Run(func(mock.Arguments) { close(unsubscribed); <-release }).Return(nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	_, err := NewClient(ctx, bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.NoError(t, err)
	cancel()

	select {
	case <-unsubscribed:
	case <-time.After(time.Second):
		t.Fatal("ctx cancel did not unsubscribe")
	}
	// paho keeps delivering until the unsubscribe completes; those sends must not block.
	for i := 0; i < 200; i++ {
		select {
		case channels[0].Messages <- types.MessageEnvelope{Payload: []byte{}}:
		case <-time.After(time.Second):
			t.Fatalf("send %d blocked: nobody drains the channel", i)
		}
	}
	close(release)
}

func TestDeviceServicesSkipsUnparsableMetric(t *testing.T) {
	good := `{"category":"XRT::DeviceService","config":{"Name":"modbus"}}`
	bad := `not json`
	p := &protobuf.Payload{Metrics: []*protobuf.Payload_Metric{
		{Name: strPtr("Service/1-xrt"), Value: &protobuf.Payload_Metric_StringValue{StringValue: bad}},
		{Name: strPtr("Service/10-modbus"), Value: &protobuf.Payload_Metric_StringValue{StringValue: good}},
	}}
	names, invalid := deviceServices(p)
	assert.Equal(t, []string{"modbus"}, names)
	assert.Equal(t, []string{"Service/1-xrt"}, invalid)
}

func strPtr(s string) *string { return &s }

func TestFailedRebirthIsRetried(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	failed := expectNodeRebirth(bus, assert.AnError, 1)
	c := newTestClient(bus)

	c.handle(message("spBv1.0/iotech/DDEATH/xrt-modbus-v3.4/modbus-sim", nil))
	waitPublished(t, failed)
	assert.Eventually(t, func() bool { return !rebornPending(c, modbusNode) }, time.Second, 5*time.Millisecond,
		"a failed publish clears the mark")

	retried := expectNodeRebirth(bus, nil, 1)
	c.handle(message("spBv1.0/iotech/DDEATH/xrt-modbus-v3.4/modbus-sim", nil))
	waitPublished(t, retried)
}

func TestDACKIsIgnored(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t)) // a rebirth publish would fail the mock
	c.handle(message("spBv1.0/iotech/DACK/xrt-modbus-v3.4/modbus-sim", mustMarshal(t, &protobuf.Payload{})))
	assert.Empty(t, c.Nodes())
	assert.Empty(t, c.reborn)
}

func TestFailedUnsubscribeKeepsDrainingAndRetries(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	var channels []types.TopicChannel
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { channels = args.Get(0).([]types.TopicChannel) }).Return(nil).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(assert.AnError).Once()
	retried := make(chan struct{})
	bus.On("Unsubscribe", anyTopics(5)...).Run(func(mock.Arguments) { close(retried) }).Return(nil).Once()

	client, err := NewClient(context.Background(), bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.NoError(t, err)
	client.(*Client).unsubRetry = 10 * time.Millisecond
	require.Error(t, client.Close())

	// The bus keeps delivering to the failed subscription (see unsubscribe).
	for i := 0; i < 200; i++ {
		select {
		case channels[0].Messages <- types.MessageEnvelope{Payload: []byte{}}:
		case <-time.After(time.Second):
			t.Fatalf("send %d blocked after a failed unsubscribe", i)
		}
	}
	select {
	case <-retried:
	case <-time.After(time.Second):
		t.Fatal("the failed unsubscribe was not retried")
	}
}

func sendAll(t *testing.T, ch chan types.MessageEnvelope, n int, msg types.MessageEnvelope) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case ch <- msg:
		case <-time.After(time.Second):
			t.Fatalf("send %d blocked", i)
		}
	}
}

func TestSlowRebirthDoesNotBlockHandling(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	var channels []types.TopicChannel
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) { channels = args.Get(0).([]types.TopicChannel) }).Return(nil).Once()
	release := make(chan struct{})
	bus.On("PublishBinaryData", mock.Anything, "spBv1.0/iotech/NCMD/xrt-modbus-v3.4").
		Run(func(mock.Arguments) { <-release }).Return(nil).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(nil).Once()

	client, err := NewClient(context.Background(), bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.NoError(t, err)
	sendAll(t, channels[0].Messages, 200, message("spBv1.0/iotech/DDEATH/xrt-modbus-v3.4/modbus-sim", nil))
	close(release)
	require.NoError(t, client.Close())
}

// Topics subscribed early deliver while SubscribeBinaryData is still running.
func TestNewClientSubscribeFailureDrains(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			channels := args.Get(0).([]types.TopicChannel)
			sendAll(t, channels[0].Messages, 200, message("spBv1.0/iotech/DACK/x/y", nil))
		}).Return(assert.AnError).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(nil).Once()

	_, err := NewClient(context.Background(), bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.Error(t, err)
}

// DBIRTH payloads carry neither bdSeq nor Service/*, so one doubles as an NBIRTH from a node without EnableServices.
func TestNBIRTHWithoutServicesOrBdSeq(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "dbirth_xrt-modbus_modbus-sim.json")))

	node := onlyNode(t, c)
	assert.Empty(t, node.Services, "kept as a node; the caller decides it has no EdgeInst")
	assert.Zero(t, node.BdSeq)
}

func TestNBIRTHReplacesServicesAndMetrics(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-bacnet.json")))

	node := onlyNode(t, c)
	assert.Equal(t, []string{"bacnet_ip"}, node.Services)
	assert.Equal(t, int64(18), node.BdSeq)
	assert.Len(t, node.Metrics, 3)
}

func TestUnknownNodeNDEATHTriggersRebirth(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	published := expectNodeRebirth(bus, nil, 1)
	c := newTestClient(bus)

	c.handle(message("spBv1.0/iotech/NDEATH/xrt-modbus-v3.4", deathPayload(t, 38)))
	waitPublished(t, published)
	assert.Empty(t, c.Nodes())
}

func TestDDEATHOfUnknownDevice(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t)) // a rebirth publish would fail the mock
	c.handle(message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json")))
	c.handle(message("spBv1.0/iotech/DBIRTH/xrt-modbus-v3.4/modbus-sim", fixture(t, "dbirth_xrt-modbus_modbus-sim.json")))
	c.handle(message("spBv1.0/iotech/DDEATH/xrt-modbus-v3.4/no-such-device", nil))

	require.Len(t, onlyNode(t, c).Devices, 1)
	assert.Empty(t, c.reborn)
}

func TestSubscriptionErrorDoesNotStopHandling(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	var channels []types.TopicChannel
	var messageErrors chan error
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		channels = args.Get(0).([]types.TopicChannel)
		messageErrors = args.Get(1).(chan error)
	}).Return(nil).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(nil).Once()

	client, err := NewClient(context.Background(), bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.NoError(t, err)
	messageErrors <- assert.AnError
	channels[0].Messages <- message("spBv1.0/iotech/NBIRTH/xrt-modbus-v3.4", fixture(t, "nbirth_xrt-modbus.json"))
	assert.Eventually(t, func() bool { return len(client.Nodes()) == 1 }, time.Second, 10*time.Millisecond)
	require.NoError(t, client.Close())
}

func TestCloseTwice(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).Return(nil).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(assert.AnError).Once()
	retried := make(chan struct{})
	bus.On("Unsubscribe", anyTopics(5)...).Run(func(mock.Arguments) { close(retried) }).Return(nil).Once()

	client, err := NewClient(context.Background(), bus, []string{"iotech"}, time.Second, logger.NewMockClient())
	require.NoError(t, err)
	client.(*Client).unsubRetry = 10 * time.Millisecond
	first := client.Close()
	require.Error(t, first)
	assert.Equal(t, first, client.Close(), "the second Close returns at once with the same result")
	<-retried // let the background retry finish inside this test
}

func TestSetRequestTimeout(t *testing.T) {
	c := newTestClient(mocks.NewMessageClient(t))
	c.SetRequestTimeout(3 * time.Second)
	assert.Equal(t, int64(3*time.Second), c.requestTimeout.Load())
}

// Cancelling during the subscription (e.g. shutdown while the broker settings change) must not leave the topics
// subscribed with no reader: the bus handler would block on the next message and stall the shared bus.
func TestNewClientCancelledWhileSubscribing(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	events := make(chan string, 4)
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).Run(func(mock.Arguments) {
		time.Sleep(50 * time.Millisecond) // a slow subscription that the cancellation overtakes
		events <- "subscribe"
	}).Return(nil).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Run(func(mock.Arguments) { events <- "unsubscribe" }).Return(nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := NewClient(ctx, bus, []string{"iotech"}, time.Second, logger.NewMockClient())

	require.Error(t, err)
	assert.Nil(t, client)
	assert.Equal(t, "subscribe", <-events)
	assert.Equal(t, "unsubscribe", <-events)
}

// Topics subscribed first deliver while the rest are pending; a cancelled client must keep draining them meanwhile.
func TestNewClientCancelledWhileSubscribingDrains(t *testing.T) {
	bus := mocks.NewMessageClient(t)
	bus.On("SubscribeBinaryData", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		channels := args.Get(0).([]types.TopicChannel)
		// DACKs are ignored, so a message run happens to handle before noticing the cancellation has no effect.
		sendAll(t, channels[0].Messages, 200, message("spBv1.0/iotech/DACK/x/y", nil))
	}).Return(nil).Once()
	bus.On("Unsubscribe", anyTopics(5)...).Return(nil).Once()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewClient(ctx, bus, []string{"iotech"}, time.Second, logger.NewMockClient())

	require.Error(t, err)
}
