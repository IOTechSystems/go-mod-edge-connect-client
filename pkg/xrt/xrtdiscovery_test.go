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

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/interfaces"
)

// Captured from XRT v4 bacnet_ip: a discovery:trigger is answered on the reply topic by this result, then by the ack.
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

const discoveryReplyTopic = "spBv1.0/iotech/REPLY/xrt-bacnet-v4/bacnet_ip"

func newDiscoveryClient(t *testing.T, bus *discoveryBus, received chan<- string) *Client {
	t.Helper()
	handler := func(message types.MessageEnvelope) { received <- string(message.Payload.([]byte)) }
	opts := NewClientOptions(nil, NewDiscoveryOptions(discoveryReplyTopic, handler, time.Second, nil, 0), nil)
	client, err := NewXrtClient(t.Context(), bus, "spBv1.0/iotech/REQUEST/xrt-bacnet-v4/bacnet_ip", discoveryReplyTopic,
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

// newBlockingDiscoveryClient's handler closes started when called, then blocks until release is closed.
func newBlockingDiscoveryClient(t *testing.T, bus *discoveryBus, started, release chan struct{}) (interfaces.EdgeClient, errors.EdgeX) {
	handler := func(types.MessageEnvelope) { close(started); <-release }
	opts := NewClientOptions(nil, NewDiscoveryOptions(discoveryReplyTopic, handler, time.Second, nil, 0), nil)
	return NewXrtClient(t.Context(), bus, "request", discoveryReplyTopic, time.Second, logger.MockLogger{}, opts)
}

func closeAsync(client interfaces.EdgeClient) <-chan errors.EdgeX {
	closed := make(chan errors.EdgeX, 1)
	go func() { closed <- client.Close() }()
	return closed
}

func TestDiscoveryOnReplyTopic_CloseWaitsForHandler(t *testing.T) {
	bus := &discoveryBus{}
	started, release := make(chan struct{}), make(chan struct{})
	client, err := newBlockingDiscoveryClient(t, bus, started, release)
	if err != nil {
		t.Fatalf("failed to create the client: %v", err)
	}
	bus.send(discoveryReplyTopic, xrtDiscoveryResult)
	<-started

	closed := closeAsync(client)
	select {
	case <-closed:
		t.Fatal("Close returned while the discovery handler was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	if err := <-closed; err != nil {
		t.Fatalf("failed to close the client: %v", err)
	}
}

// The next discovery on the same reply topic can start while the previous client is still closing.
func TestDiscoveryOnReplyTopic_CloseDoesNotWaitForNextHandler(t *testing.T) {
	bus := &discoveryBus{}
	startedA, releaseA := make(chan struct{}), make(chan struct{})
	a, err := newBlockingDiscoveryClient(t, bus, startedA, releaseA)
	if err != nil {
		t.Fatalf("failed to create the client: %v", err)
	}
	bus.send(discoveryReplyTopic, xrtDiscoveryResult)
	<-startedA
	closedA := closeAsync(a)

	startedB, releaseB := make(chan struct{}), make(chan struct{})
	var b interfaces.EdgeClient
	deadline := time.Now().Add(time.Second)
	for { // retry until a has cleared its handler
		if b, err = newBlockingDiscoveryClient(t, bus, startedB, releaseB); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the next discovery could not start: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer func() { close(releaseB); _ = b.Close() }()
	bus.send(discoveryReplyTopic, xrtDiscoveryResult)
	<-startedB

	close(releaseA)
	select {
	case err := <-closedA:
		if err != nil {
			t.Fatalf("failed to close the client: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close waited for the next client's discovery handler")
	}
}
