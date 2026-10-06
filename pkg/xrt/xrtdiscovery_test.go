// Copyright (C) 2026 IOTech Ltd

package xrt

import (
	"encoding/json"
	goerrors "errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// Captured from XRT 3.4.6 bacnet_ip: a discovery:trigger is answered on the reply topic by this result, then by the ack.
const (
	xrtDiscoveryResult = `{"devices":{"BacnetSimulator1:1234":{"properties":{"IP":"172.20.0.5","InstanceID":1234},` +
		`"protocols":{"BACnet-IP":{"DeviceInstance":1234}}}},"type":"xrt.device.discovery:1.0"}`
	xrtDiscoveryAck = `{"client":"probe","request_id":"%s","result":{"status":0},"type":"xrt.reply:1.0"}`
)

// discoveryBus answers every publish on the reply topic with xrtDiscoveryResult followed by the ack.
type discoveryBus struct {
	messaging.MessageClient
	mutex    sync.Mutex
	channels map[string]chan types.MessageEnvelope
}

func (bus *discoveryBus) SubscribeBinaryData(topics []types.TopicChannel, _ chan error) error {
	bus.mutex.Lock()
	defer bus.mutex.Unlock()
	if bus.channels == nil {
		bus.channels = make(map[string]chan types.MessageEnvelope)
	}
	for _, topic := range topics {
		bus.channels[topic.Topic] = topic.Messages
	}
	return nil
}

func (bus *discoveryBus) Unsubscribe(...string) error { return nil }

func (bus *discoveryBus) PublishBinaryData(payload []byte, _ string) error {
	var request struct {
		RequestId string `json:"request_id"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	go func() {
		bus.send(discoveryReplyTopic, xrtDiscoveryResult)
		bus.send(discoveryReplyTopic, fmt.Sprintf(xrtDiscoveryAck, request.RequestId))
	}()
	return nil
}

func (bus *discoveryBus) send(topic, payload string) {
	bus.mutex.Lock()
	ch := bus.channels[topic]
	bus.mutex.Unlock()
	ch <- types.MessageEnvelope{Payload: []byte(payload), ContentType: common.ContentTypeJSON, ReceivedTopic: topic}
}

const discoveryReplyTopic = "spBv1.0/iotech/REPLY/xrt-bacnet-v3.4/bacnet_ip"

func newDiscoveryClient(t *testing.T, bus *discoveryBus, received chan<- string) *Client {
	t.Helper()
	handler := func(message types.MessageEnvelope) { received <- string(message.Payload.([]byte)) }
	opts := NewClientOptions(nil, NewDiscoveryOptions(discoveryReplyTopic, handler, time.Second, nil, 0), nil)
	client, err := NewXrtClient(t.Context(), bus, "spBv1.0/iotech/REQUEST/xrt-bacnet-v3.4/bacnet_ip", discoveryReplyTopic,
		time.Second, logger.MockLogger{}, opts)
	if err != nil {
		t.Fatalf("failed to create the client: %v", err)
	}
	return client.(*Client)
}

func TestDiscoveryOnReplyTopic(t *testing.T) {
	bus := &discoveryBus{}
	received := make(chan string, 4)
	client := newDiscoveryClient(t, bus, received)
	defer func() { _ = client.Close() }()

	if err := client.TriggerDiscovery(t.Context()); err != nil {
		t.Fatalf("the trigger must get its ack: %v", err)
	}

	select {
	case got := <-received:
		if got != xrtDiscoveryResult {
			t.Fatalf("the discovery handler got %s", got)
		}
	case <-time.After(time.Second):
		t.Fatal("the discovery result was not passed to the discovery handler")
	}
	select {
	case got := <-received:
		t.Fatalf("only xrt.device.discovery messages may reach the discovery handler, got %s", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// Reply managers are shared per topic, so a closed client's handler must not outlive it.
func TestDiscoveryOnReplyTopic_CloseClearsHandler(t *testing.T) {
	bus := &discoveryBus{}
	received := make(chan string, 4)
	closed := newDiscoveryClient(t, bus, received)
	remaining, err := NewXrtClient(t.Context(), bus, "request", discoveryReplyTopic, time.Second, logger.MockLogger{}, nil)
	if err != nil {
		t.Fatalf("failed to create the client: %v", err)
	}
	defer func() { _ = remaining.Close() }()

	if err := closed.Close(); err != nil {
		t.Fatalf("failed to close the client: %v", err)
	}
	bus.send(discoveryReplyTopic, xrtDiscoveryResult)

	select {
	case got := <-received:
		t.Fatalf("a closed client's discovery handler was called with %s", got)
	case <-time.After(100 * time.Millisecond):
	}
}

// The rejected client must not clear the running discovery's handler when it cleans up.
func TestDiscoveryOnReplyTopic_SecondDiscoveryRejected(t *testing.T) {
	bus := &discoveryBus{}
	received := make(chan string, 4)
	running := newDiscoveryClient(t, bus, received)
	defer func() { _ = running.Close() }()

	handler := func(types.MessageEnvelope) { t.Error("the rejected client's handler was called") }
	opts := NewClientOptions(nil, NewDiscoveryOptions(discoveryReplyTopic, handler, time.Second, nil, 0), nil)
	_, err := NewXrtClient(t.Context(), bus, "request", discoveryReplyTopic, time.Second, logger.MockLogger{}, opts)
	var edgexErr errors.EdgeX
	if !goerrors.As(err, &edgexErr) || edgexErr.Code() != 409 {
		t.Fatalf("a second discovery on the same reply topic must be a conflict, got %v", err)
	}

	bus.send(discoveryReplyTopic, xrtDiscoveryResult)
	select {
	case <-received:
	case <-time.After(time.Second):
		t.Fatal("the running discovery lost its handler")
	}
}
