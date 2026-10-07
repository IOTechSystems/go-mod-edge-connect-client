// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	spb "github.com/IOTechSystems/sparkplug-sdk-go/pkg/sparkplug"
	"github.com/IOTechSystems/sparkplug-sdk-go/pkg/sparkplug/payload"
	"github.com/IOTechSystems/sparkplug-sdk-go/pkg/sparkplug/protobuf"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"google.golang.org/protobuf/proto"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/interfaces"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
)

const (
	namespace = "spBv1.0"
	// msgTypeDACK is XRT's acknowledgement of a DCMD; sparkplug-sdk-go has no constant for it.
	msgTypeDACK = "DACK"
)

// defaultUnsubscribeRetry paces the retries after a failed unsubscribe.
const defaultUnsubscribeRetry = 5 * time.Second

// Client implements interfaces.SparkplugClient.
type Client struct {
	lc             logger.LoggingClient
	messageBus     messaging.MessageClient
	groups         []string
	topics         []string
	requestTimeout atomic.Int64       // DCMD -> DACK, in nanoseconds; unused until DCMD is implemented
	cancel         context.CancelFunc // stops the message goroutine, which then unsubscribes
	done           chan struct{}      // closed when the message goroutine has unsubscribed and exited
	closeErr       errors.EdgeX       // unsubscribe result; written before done is closed
	unsubRetry     time.Duration      // retry interval after a failed unsubscribe

	nodesMu sync.RWMutex
	nodes   map[models.NodeKey]models.NodeInfo
	reborn  map[models.NodeKey]struct{} // unknown nodes already sent a rebirth; cleared by their NBIRTH
}

// NewClient subscribes to NBIRTH/NDEATH/DBIRTH/DDEATH/DACK of the groups; it does not publish a rebirth.
// requestTimeout is how long a DCMD waits for its DACK.
func NewClient(ctx context.Context, messageBus messaging.MessageClient, groups []string,
	requestTimeout time.Duration, lc logger.LoggingClient) (interfaces.SparkplugClient, errors.EdgeX) {
	if len(groups) == 0 {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "at least one Sparkplug group is required", nil)
	}

	messages := make(chan types.MessageEnvelope)
	messageErrors := make(chan error, 1)
	topics := subscribeTopics(groups)
	runCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c := &Client{
		lc:         lc,
		messageBus: messageBus,
		groups:     slices.Clone(groups),
		topics:     topics,
		cancel:     cancel,
		done:       make(chan struct{}),
		unsubRetry: defaultUnsubscribeRetry,
		nodes:      make(map[models.NodeKey]models.NodeInfo),
		reborn:     make(map[models.NodeKey]struct{}),
	}
	c.requestTimeout.Store(int64(requestTimeout))
	// Started before subscribing: topics subscribed early deliver while the remaining ones are still pending.
	go c.run(runCtx, messages, messageErrors)

	channels := make([]types.TopicChannel, 0, len(topics))
	for _, topic := range topics {
		channels = append(channels, types.TopicChannel{Topic: topic, Messages: messages})
	}
	if err := messageBus.SubscribeBinaryData(channels, messageErrors); err != nil {
		// Topics are subscribed one by one, so those before the failure are live; run unsubscribes them.
		c.closeAndLog()
		return nil, errors.NewCommonEdgeX(errors.KindCommunicationError, "failed to subscribe to Sparkplug BIRTH/DEATH/DACK", err)
	}
	if ctx.Err() != nil {
		c.closeAndLog()
		return nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, "cancelled while subscribing to Sparkplug topics", ctx.Err())
	}
	go c.closeOnDone(ctx)
	return c, nil
}

func subscribeTopics(groups []string) []string {
	topics := make([]string, 0, len(groups)*5)
	for _, g := range groups {
		topics = append(topics,
			fmt.Sprintf("%s/%s/%s/+", namespace, g, spb.NBIRTH),
			fmt.Sprintf("%s/%s/%s/+", namespace, g, spb.NDEATH),
			fmt.Sprintf("%s/%s/%s/+/+", namespace, g, spb.DBIRTH),
			fmt.Sprintf("%s/%s/%s/+/+", namespace, g, spb.DDEATH),
			fmt.Sprintf("%s/%s/%s/+/+", namespace, g, msgTypeDACK),
		)
	}
	return topics
}

// run handles every message in one goroutine, so NBIRTH is always applied before the DBIRTHs that follow it.
// It stops when ctx is done, whether by Close or by the caller, and unsubscribes first.
func (c *Client) run(ctx context.Context, messages <-chan types.MessageEnvelope, messageErrors <-chan error) {
	defer close(c.done)
	for {
		select {
		case <-ctx.Done():
			c.closeErr = c.unsubscribe(messages, messageErrors)
			return
		case err := <-messageErrors:
			c.lc.Errorf("Sparkplug subscription error: %v", err)
		case msg := <-messages:
			c.handle(msg)
		}
	}
}

func (c *Client) handle(msg types.MessageEnvelope) {
	data, ok := msg.Payload.([]byte)
	if !ok {
		c.lc.Warnf("Sparkplug message on %s has a %T payload, expected []byte", msg.ReceivedTopic, msg.Payload)
		return
	}
	_, group, msgType, node, device, err := spb.ParseSparkplugTopic(msg.ReceivedTopic)
	if err != nil {
		c.lc.Warnf("skip Sparkplug message: %v", err)
		return
	}
	// DACKs answer DCMDs, which are not implemented yet; they must not reach apply, where a DACK from a node not
	// yet known would trigger a rebirth.
	if msgType == msgTypeDACK {
		c.lc.Debugf("ignore Sparkplug DACK on %s", msg.ReceivedTopic)
		return
	}
	var p protobuf.Payload
	if err := proto.Unmarshal(data, &p); err != nil {
		c.lc.Warnf("failed to decode Sparkplug payload on %s: %v", msg.ReceivedTopic, err)
		return
	}

	key := models.NodeKey{Group: group, Node: node}
	if c.applyOrMarkRebirth(key, msgType, device, &p) {
		// Published in the background: a publish can wait up to the bus timeout, and message handling must not stop.
		go c.rebirthNode(key)
	}
}

// applyOrMarkRebirth updates the state of a known node, or marks an unknown one for a rebirth (see markReborn).
func (c *Client) applyOrMarkRebirth(key models.NodeKey, msgType, device string, p *protobuf.Payload) (needsRebirth bool) {
	c.nodesMu.Lock()
	defer c.nodesMu.Unlock()

	if msgType == spb.NBIRTH {
		c.applyNBIRTH(key, p)
		return false
	}
	info, known := c.nodes[key]
	if !known {
		return c.markReborn(key)
	}
	switch msgType {
	case spb.NDEATH:
		c.applyNDEATH(info, p)
	case spb.DBIRTH:
		known := hasDevice(info.Devices, device)
		info.Devices = replaceDevice(info.Devices, models.Device{Name: device, Metrics: metricDefs(p)})
		c.nodes[key] = info
		if !known {
			c.lc.Debugf("Sparkplug device %s added to %s/%s; the node now has %d devices", device, key.Group, key.Node, len(info.Devices))
		}
	case spb.DDEATH:
		if !hasDevice(info.Devices, device) {
			break
		}
		info.Devices = removeDevice(info.Devices, device)
		c.nodes[key] = info
		c.lc.Debugf("Sparkplug device %s removed from %s/%s; %d devices remain on the node", device, key.Group, key.Node, len(info.Devices))
	}
	return false
}

func (c *Client) applyNBIRTH(key models.NodeKey, p *protobuf.Payload) {
	seq, ok := bdSeq(p)
	if !ok {
		c.lc.Warnf("Sparkplug NBIRTH of %s/%s has no bdSeq", key.Group, key.Node)
	}
	services, invalid := deviceServices(p)
	for _, name := range invalid {
		c.lc.Warnf("skip unparsable metric %s of %s/%s", name, key.Group, key.Node)
	}
	if len(services) == 0 {
		c.lc.Warnf("Sparkplug node %s/%s lists no device service; is XRT EnableServices on?", key.Group, key.Node)
	}
	_, known := c.nodes[key]
	c.nodes[key] = models.NodeInfo{NodeKey: key, BdSeq: seq, Services: services, Metrics: metricDefs(p)}
	delete(c.reborn, key)
	if !known {
		c.lc.Infof("Sparkplug node %s/%s added with device services %v; %d nodes are now online", key.Group, key.Node, services, len(c.nodes))
	}
}

func (c *Client) applyNDEATH(info models.NodeInfo, p *protobuf.Payload) {
	seq, ok := bdSeq(p)
	if !ok || !isSameOrNewerBdSeq(info.BdSeq, seq) {
		c.lc.Debugf("ignore stale Sparkplug NDEATH of %s/%s", info.Group, info.Node)
		return
	}
	delete(c.nodes, info.NodeKey)
	c.lc.Infof("Sparkplug node %s/%s removed; %d nodes remain online", info.Group, info.Node, len(c.nodes))
}

// markReborn reports whether an unknown node still needs a rebirth: only once until its NBIRTH arrives.
func (c *Client) markReborn(key models.NodeKey) bool {
	if _, done := c.reborn[key]; done {
		return false
	}
	c.reborn[key] = struct{}{}
	return true
}

func replaceDevice(devices []models.Device, device models.Device) []models.Device {
	out := make([]models.Device, 0, len(devices)+1)
	for _, d := range devices {
		if d.Name != device.Name {
			out = append(out, d)
		}
	}
	return append(out, device)
}

func hasDevice(devices []models.Device, name string) bool {
	return slices.ContainsFunc(devices, func(d models.Device) bool { return d.Name == name })
}

func removeDevice(devices []models.Device, name string) []models.Device {
	return slices.DeleteFunc(slices.Clone(devices), func(d models.Device) bool { return d.Name == name })
}

func (c *Client) Nodes() []models.NodeInfo {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()

	nodes := make([]models.NodeInfo, 0, len(c.nodes))
	for _, n := range c.nodes {
		nodes = append(nodes, n)
	}
	slices.SortFunc(nodes, func(a, b models.NodeInfo) int {
		return cmp.Or(cmp.Compare(a.Group, b.Group), cmp.Compare(a.Node, b.Node))
	})
	return nodes
}

func (c *Client) Rebirth() errors.EdgeX {
	var firstErr errors.EdgeX
	for _, g := range c.groups {
		topic := fmt.Sprintf("%s/%s/%s", namespace, g, spb.NCMD)
		if err := c.publishRebirth(topic); err != nil {
			c.lc.Error(err.Error())
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

func (c *Client) rebirthNode(key models.NodeKey) {
	topic := fmt.Sprintf("%s/%s/%s/%s", namespace, key.Group, spb.NCMD, key.Node)
	if err := c.publishRebirth(topic); err != nil {
		c.lc.Error(err.Error())
		c.nodesMu.Lock()
		delete(c.reborn, key) // retry on the next message from this node
		c.nodesMu.Unlock()
	}
}

func (c *Client) publishRebirth(topic string) errors.EdgeX {
	data, err := proto.Marshal(payload.NewNodeRebirth())
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to encode the Sparkplug rebirth", err)
	}
	if err := c.messageBus.PublishBinaryData(data, topic); err != nil {
		return errors.NewCommonEdgeX(errors.KindCommunicationError, fmt.Sprintf("failed to publish rebirth to %s", topic), err)
	}
	return nil
}

func (c *Client) ReadDeviceResources(_ context.Context, _ models.NodeKey, _ string,
	_ []string) (xrtmodels.MultiResourcesResult, errors.EdgeX) {
	return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeX(errors.KindNotImplemented, "DCMD read is not implemented yet", nil)
}

func (c *Client) WriteDeviceResources(_ context.Context, _ models.NodeKey, _ string, _, _ map[string]any) errors.EdgeX {
	return errors.NewCommonEdgeX(errors.KindNotImplemented, "DCMD write is not implemented yet", nil)
}

func (c *Client) SetRequestTimeout(requestTimeout time.Duration) {
	c.requestTimeout.Store(int64(requestTimeout))
}

// closeOnDone closes the client when callerCtx, the ctx given to NewClient, is done, unless it is closed first.
func (c *Client) closeOnDone(callerCtx context.Context) {
	select {
	case <-callerCtx.Done():
		c.closeAndLog()
	case <-c.done:
	}
}

func (c *Client) closeAndLog() {
	if err := c.Close(); err != nil {
		c.lc.Errorf("failed to close the Sparkplug client: %v", err)
	}
}

// Close must be called before the shared MessageClient is disconnected; otherwise the unsubscribe keeps failing and
// is retried in the background for as long as the process runs (see unsubscribe).
func (c *Client) Close() errors.EdgeX {
	c.cancel()
	<-c.done
	return c.closeErr
}

// unsubscribe removes the subscriptions while draining their channels: the bus handler blocks until each message is
// read, and a blocked handler stalls every subscription that shares the bus.
//
// A failed unsubscribe (e.g. while disconnected) leaves the subscriptions in the bus, which re-creates them on
// reconnect. Draining and retrying then continue in the background until an unsubscribe succeeds.
func (c *Client) unsubscribe(messages <-chan types.MessageEnvelope, messageErrors <-chan error) errors.EdgeX {
	err := c.unsubscribeDraining(messages, messageErrors)
	if err == nil {
		return nil
	}
	go c.retryUnsubscribe(messages, messageErrors)
	return errors.NewCommonEdgeX(errors.KindCommunicationError, "failed to unsubscribe from Sparkplug BIRTH/DEATH/DACK", err)
}

func (c *Client) unsubscribeDraining(messages <-chan types.MessageEnvelope, messageErrors <-chan error) error {
	result := make(chan error, 1)
	go func() { result <- c.messageBus.Unsubscribe(c.topics...) }()
	for {
		select {
		case err := <-result:
			return err
		case <-messages:
		case <-messageErrors:
		}
	}
}

func (c *Client) retryUnsubscribe(messages <-chan types.MessageEnvelope, messageErrors <-chan error) {
	ticker := time.NewTicker(c.unsubRetry)
	defer ticker.Stop()
	for {
		select {
		case <-messages:
		case <-messageErrors:
		case <-ticker.C:
			if err := c.unsubscribeDraining(messages, messageErrors); err == nil {
				return
			}
		}
	}
}
