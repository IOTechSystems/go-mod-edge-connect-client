// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"
	"uuid"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/common"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"google.golang.org/protobuf/proto"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/models"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/sparkplug/protobuf"
)

const (
	// qualityProperty is an OPC UA StatusCode (Part 4, 7.39); e.g. XRT rejects a write to a read-only resource with
	// 0x80890000 (Bad_ConfigurationError).
	qualityProperty = "quality"
	qualityNotGood  = 0xC0000000 // severity bits: set for Uncertain or Bad, both of which fail the command
)

// qualityCode is a quality code that XRT devsdk sets (DEVSDK_QUALITY_*). kind is empty for KindServerError.
type qualityCode struct {
	name    string
	message string
	kind    errors.ErrKind
}

// qualityCodes are the Uncertain and Bad quality codes that XRT devsdk sets; the kinds are those the caller can fix.
var qualityCodes = map[uint32]qualityCode{
	0x40900000: {"Uncertain_LastUsableValue", "the device returned only its last usable value", ""},
	0x40930000: {"Uncertain_SensorNotAccurate", "the sensor value may not be accurate", ""},
	0x40940000: {"Uncertain_EngineeringUnitsExceeded", "the value exceeds the engineering unit limits", ""},
	0x80310000: {"Bad_NoCommunication", "unable to communicate with the device", ""},
	0x808B0000: {"Bad_DeviceFailure", "the device reported a failure", ""},
	// as for a read-only resource rejected before sending
	0x80890000: {"Bad_ConfigurationError", "the device or resource configuration does not allow this command", errors.KindNotAllowed},
	0x80320000: {"Bad_WaitingForInitialData", "the device has not provided a value yet", ""},
	0x803C0000: {"Bad_OutOfRange", "the value is out of range", errors.KindContractInvalid},
	0x80740000: {"Bad_TypeMismatch", "the value type does not match the resource", errors.KindContractInvalid},
	0x80E30000: {"Bad_DependentValueChanged", "not written because another value in the same command was invalid", ""},
}

func (c *Client) ReadDeviceResources(ctx context.Context, node models.NodeKey, device string,
	resourceNames []string) (xrtmodels.MultiResourcesResult, errors.EdgeX) {
	if len(resourceNames) == 0 {
		return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeX(errors.KindContractInvalid, "no resource to read", nil)
	}
	// find the specified device
	dev, err := c.onlineDevice(node, device)
	if err != nil {
		return xrtmodels.MultiResourcesResult{}, err
	}
	// match the metrics with resourceNames, and create read metric to send the request
	matchedMetricDefs, metrics := make([]models.MetricDef, 0, len(resourceNames)), make([]*protobuf.Payload_Metric, 0, len(resourceNames))
	for _, resourceName := range resourceNames {
		def, err := resourceMetricDef(dev.Metrics, device, resourceName, common.ReadWrite_R)
		if err != nil {
			return xrtmodels.MultiResourcesResult{}, err
		}
		matchedMetricDefs, metrics = append(matchedMetricDefs, def), append(metrics, readMetric(def))
	}

	dack, resourceMetrics, err := c.request(ctx, node, device, matchedMetricDefs, metrics, true)
	if err != nil {
		return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeXWrapper(err)
	}
	result := xrtmodels.MultiResourcesResult{Device: device, Profile: dev.Profile,
		Readings: make(map[string]xrtmodels.Reading, len(matchedMetricDefs))}
	for _, def := range matchedMetricDefs {
		reading, err := newReading(def, resourceMetrics[def.Name], dack)
		if err != nil {
			err = commandFailed(def, true, err.Error())
			c.lc.Errorf("Sparkplug DCMD to %s failed: %v", dcmdTopic(node, device), err)
			return xrtmodels.MultiResourcesResult{}, err
		}
		result.Readings[def.Name] = reading
	}
	return result, nil
}

// WriteDeviceResources accepts no options: a DCMD has nowhere to carry them.
func (c *Client) WriteDeviceResources(ctx context.Context, node models.NodeKey, device string,
	resourceValuePairs, options map[string]any) errors.EdgeX {
	if len(options) > 0 {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "a Sparkplug DCMD write takes no options", nil)
	}
	if len(resourceValuePairs) == 0 {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "no resource to write", nil)
	}
	// find the specified device
	dev, err := c.onlineDevice(node, device)
	if err != nil {
		return err
	}
	// match the metrics with resourceNames, and create write metric to send the request
	matchedMetricDefs, metrics := make([]models.MetricDef, 0, len(resourceValuePairs)), make([]*protobuf.Payload_Metric, 0, len(resourceValuePairs))
	for _, resourceName := range slices.Sorted(maps.Keys(resourceValuePairs)) {
		def, err := resourceMetricDef(dev.Metrics, device, resourceName, common.ReadWrite_W)
		if err != nil {
			return err
		}
		m, err := writeMetric(def, resourceValuePairs[resourceName])
		if err != nil {
			return err
		}
		matchedMetricDefs, metrics = append(matchedMetricDefs, def), append(metrics, m)
	}

	if _, _, err := c.request(ctx, node, device, matchedMetricDefs, metrics, false); err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	return nil
}

// onlineDevice returns the device from its latest DBIRTH; its slices are never modified in place (see NodeInfo).
func (c *Client) onlineDevice(node models.NodeKey, device string) (models.Device, errors.EdgeX) {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()

	nodeInfo, ok := c.nodes[node]
	if !ok {
		return models.Device{}, errors.NewCommonEdgeX(errors.KindEntityDoesNotExist, fmt.Sprintf("Sparkplug node %s/%s is not online", node.Group, node.Node), nil)
	}
	i := slices.IndexFunc(nodeInfo.Devices, func(d models.Device) bool { return d.Name == device })
	if i < 0 {
		return models.Device{}, errors.NewCommonEdgeX(errors.KindEntityDoesNotExist,
			fmt.Sprintf("device %s is not online on Sparkplug node %s/%s", device, node.Group, node.Node), nil)
	}
	return nodeInfo.Devices[i], nil
}

// resourceMetricDef returns the DBIRTH definition of a resource, checked before sending so that an unknown or
// unwritable one fails with its own message. access is common.ReadWrite_R or common.ReadWrite_W.
func resourceMetricDef(defs []models.MetricDef, device, name, access string) (models.MetricDef, errors.EdgeX) {
	i := slices.IndexFunc(defs, func(d models.MetricDef) bool { return d.Name == name })
	if i < 0 {
		return models.MetricDef{}, errors.NewCommonEdgeX(errors.KindEntityDoesNotExist, fmt.Sprintf("device %s has no resource %s", device, name), nil)
	}
	def := defs[i]
	if !strings.Contains(def.ReadWrite, access) {
		return models.MetricDef{}, errors.NewCommonEdgeX(errors.KindNotAllowed,
			fmt.Sprintf("resource %s of device %s has readWrite %q, which does not allow %s", name, device, def.ReadWrite, access), nil)
	}
	if _, ok := valueTypes[protobuf.DataType(def.Datatype)]; !ok {
		return models.MetricDef{}, errors.NewCommonEdgeX(errors.KindNotImplemented,
			fmt.Sprintf("resource %s has datatype %s, which DCMD does not support yet", name, protobuf.DataType(def.Datatype)), nil)
	}
	if access == common.ReadWrite_W && protobuf.DataType(def.Datatype) == protobuf.DataType_Bytes {
		return models.MetricDef{}, errors.NewCommonEdgeX(errors.KindNotImplemented, fmt.Sprintf("writing the Binary resource %s is not supported", name), nil)
	}
	// Without an alias its DACK metric cannot be matched, so its result would be unknown (see ackedMetrics).
	if def.Alias == 0 {
		return models.MetricDef{}, errors.NewCommonEdgeX(errors.KindNotImplemented,
			fmt.Sprintf("resource %s has no alias in the DBIRTH, which DCMD needs to match its DACK", name), nil)
	}
	return def, nil
}

// request publishes a DCMD of metrics, one per def, and returns the DACK with its metric of each def by name. A missing
// DACK cannot be told apart from a slow one, so a timed out write has an unknown result.
func (c *Client) request(ctx context.Context, node models.NodeKey, device string, defs []models.MetricDef,
	metrics []*protobuf.Payload_Metric, isRead bool) (*protobuf.Payload, map[string]*protobuf.Payload_Metric, errors.EdgeX) {
	timeout := time.Duration(c.requestTimeout.Load())
	if err := c.checkSendable(ctx, timeout); err != nil {
		return nil, nil, err
	}
	id := uuid.New().String()
	dcmd := newDCMD(id, metrics)
	data, err := proto.Marshal(dcmd)
	if err != nil {
		return nil, nil, errors.NewCommonEdgeX(errors.KindServerError, "failed to encode the Sparkplug DCMD", err)
	}
	// Registered before publishing: the DACK can arrive before PublishBinaryData returns.
	waiter := c.addDACKWaiter(id)
	defer c.removeDACKWaiter(id)

	topic := dcmdTopic(node, device)
	// The payload is formatted only if debug logging is on.
	c.lc.Debugf("Publish Sparkplug DCMD to %s: %v", topic, dcmd)
	if err := c.messageBus.PublishBinaryData(data, topic); err != nil {
		return nil, nil, errors.NewCommonEdgeX(errors.KindCommunicationError, fmt.Sprintf("failed to publish DCMD to %s", topic), err)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case dack := <-waiter:
		// handle the DACK
		resourceMetrics, err := c.ackedMetrics(topic, dack, defs, isRead)
		return dack, resourceMetrics, err
	case <-ctx.Done():
		return nil, nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, fmt.Sprintf("cancelled while waiting for the DACK of %s", topic), ctx.Err())
	case <-c.done:
		return nil, nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, "the Sparkplug client is closed", nil)
	case <-timer.C:
		return nil, nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, fmt.Sprintf("no DACK of %s within %v", topic, timeout), nil)
	}
}

func dcmdTopic(node models.NodeKey, device string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", namespace, node.Group, msgTypeDCMD, node.Node, device)
}

// checkSendable rejects a DCMD that could not be answered, so that a write the caller is told has failed was never
// sent. A Close that races with the publish can still let one through.
func (c *Client) checkSendable(ctx context.Context, timeout time.Duration) errors.EdgeX {
	if timeout <= 0 {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("the request timeout %v is not positive", timeout), nil)
	}
	if ctx.Err() != nil {
		return errors.NewCommonEdgeX(errors.KindServiceUnavailable, "cancelled before sending the DCMD", ctx.Err())
	}
	select {
	case <-c.done:
		return errors.NewCommonEdgeX(errors.KindServiceUnavailable, "the Sparkplug client is closed", nil)
	default:
		return nil
	}
}

func (c *Client) addDACKWaiter(id string) <-chan *protobuf.Payload {
	waiter := make(chan *protobuf.Payload, 1)
	c.dackWaitersMu.Lock()
	c.dackWaiters[id] = waiter
	c.dackWaitersMu.Unlock()
	return waiter
}

func (c *Client) removeDACKWaiter(id string) {
	c.dackWaitersMu.Lock()
	delete(c.dackWaiters, id)
	c.dackWaitersMu.Unlock()
}

// deliverDACK hands a DACK to the request waiting for its CommandId. The entry is removed as it is taken, so the
// buffered send happens once and never blocks, even if the broker delivers the DACK twice.
func (c *Client) deliverDACK(topic string, p *protobuf.Payload) {
	c.lc.Debugf("Received Sparkplug DACK on %s: %v", topic, p)
	id := commandID(p)
	c.dackWaitersMu.Lock()
	waiter, ok := c.dackWaiters[id]
	delete(c.dackWaiters, id)
	c.dackWaitersMu.Unlock()
	if !ok {
		c.lc.Debugf("Ignore Sparkplug DACK on %s: no request is waiting for CommandId %q", topic, id)
		return
	}
	waiter <- p
}

// newDCMD returns a DCMD carrying metrics. AckGood asks for the value of each metric in the DACK (XRT DCMD.pdf), so
// the DACK alone answers the request.
func newDCMD(commandID string, metrics []*protobuf.Payload_Metric) *protobuf.Payload {
	ts := uint64(time.Now().UnixMilli())
	return &protobuf.Payload{
		Timestamp: &ts,
		Metrics: append([]*protobuf.Payload_Metric{
			{
				Name:     proto.String(commandIDMetricName),
				Datatype: proto.Uint32(uint32(protobuf.DataType_UUID)),
				Value:    &protobuf.Payload_Metric_StringValue{StringValue: commandID},
			},
			{
				Name:     proto.String(ackGoodMetricName),
				Datatype: proto.Uint32(uint32(protobuf.DataType_Boolean)),
				Value:    &protobuf.Payload_Metric_BooleanValue{BooleanValue: true},
			},
		}, metrics...),
	}
}

// readMetric requests the value of def; XRT treats a metric as a read only if is_null is explicitly true.
func readMetric(def models.MetricDef) *protobuf.Payload_Metric {
	return &protobuf.Payload_Metric{Name: proto.String(def.Name), Datatype: proto.Uint32(def.Datatype), IsNull: proto.Bool(true)}
}

// writeMetric encodes value as the DBIRTH datatype of def, the only datatype of the resource the client knows.
func writeMetric(def models.MetricDef, value any) (*protobuf.Payload_Metric, errors.EdgeX) {
	encoded := value
	if isObject(def) {
		s, err := objectJSON(value)
		if err != nil {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, fmt.Sprintf("invalid Object value %v for %s", value, def.Name), err)
		}
		encoded = s
	}
	m := &protobuf.Payload_Metric{Name: proto.String(def.Name), Datatype: proto.Uint32(def.Datatype)}
	if err := setValue(m, protobuf.DataType(def.Datatype), encoded); err != nil {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid,
			fmt.Sprintf("invalid value %v for %s of type %s", value, def.Name, edgexValueType(def)), err)
	}
	return m, nil
}

// ackedMetrics returns the DACK metric of each def, matched by alias; a def missing from the DACK or failed fails the
// whole request.
func (c *Client) ackedMetrics(topic string, dack *protobuf.Payload, defs []models.MetricDef, isRead bool) (map[string]*protobuf.Payload_Metric, errors.EdgeX) {
	resourceMetrics := make(map[string]*protobuf.Payload_Metric, len(defs))
	var messages []string
	var kind errors.ErrKind
	for _, def := range defs {
		resourceAck := findMetricByAlias(dack, def.Alias)
		if err := validateResourceAck(def, resourceAck, isRead); err != nil {
			messages = append(messages, err.Error())
			kind = mergeKind(kind, errors.Kind(err))
			continue
		}
		resourceMetrics[def.Name] = resourceAck
	}
	if len(messages) > 0 {
		message := strings.Join(messages, "; ")
		c.lc.Errorf("Sparkplug DCMD to %s failed: %s", topic, message)
		return nil, errors.NewCommonEdgeX(kind, message, nil)
	}
	return resourceMetrics, nil
}

// validateResourceAck returns the error of the DACK metric of def if it failed; resourceAck is nil if the DACK lacks
// it. A write is judged by its quality alone, since XRT was seen to answer both a value it could not convert and a
// null value with is_null.
func validateResourceAck(def models.MetricDef, resourceAck *protobuf.Payload_Metric, isRead bool) errors.EdgeX {
	switch {
	case resourceAck == nil:
		return commandFailed(def, isRead, "missing from the DACK")
	case resourceAck.GetName() != "" && resourceAck.GetName() != def.Name:
		return commandFailed(def, isRead, fmt.Sprintf("its alias belongs to %s in the DACK; the DBIRTH may be stale", resourceAck.GetName()))
	}
	if q := quality(resourceAck); q&qualityNotGood != 0 {
		return qualityFailed(def, isRead, q)
	}
	if !isRead {
		return nil
	}
	if isObject(def) {
		if hasValue[*protobuf.Payload_Metric_StringValue](resourceAck) && !json.Valid([]byte(resourceAck.GetStringValue())) {
			return commandFailed(def, isRead, "the value of an Object is not JSON")
		}
		return nil
	}
	if _, ok := readingValue(protobuf.DataType(def.Datatype), resourceAck); !ok {
		return commandFailed(def, isRead, fmt.Sprintf("no value of the DBIRTH datatype %s", protobuf.DataType(def.Datatype)))
	}
	return nil
}

func commandFailed(def models.MetricDef, isRead bool, reason string) errors.EdgeX {
	return errors.NewCommonEdgeX(errors.KindServerError, fmt.Sprintf("%s: %s (%s)", def.Name, operationFailed(isRead), reason), nil)
}

// qualityFailed points to the XRT log, since the DACK carries no cause of a quality code, e.g. the BACnet error.
func qualityFailed(def models.MetricDef, isRead bool, q uint32) errors.EdgeX {
	message, detail := operationFailed(isRead), fmt.Sprintf("quality 0x%08X", q)
	code, known := qualityCodes[q]
	if known {
		message, detail = message+", "+code.message, code.name+", "+detail
	}
	return errors.NewCommonEdgeX(cmp.Or(code.kind, errors.KindServerError),
		fmt.Sprintf("%s: %s (%s); see the XRT log for the cause", def.Name, message, detail), nil)
}

func operationFailed(isRead bool) string {
	if isRead {
		return "Read failed"
	}
	return "Write failed"
}

// mergeKind keeps a kind shared by every failed resource; failures of different kinds make a server error.
func mergeKind(kind, next errors.ErrKind) errors.ErrKind {
	if kind == "" || kind == next {
		return next
	}
	return errors.KindServerError
}

func quality(m *protobuf.Payload_Metric) uint32 {
	return property(m.GetProperties(), qualityProperty).GetIntValue()
}

// findMetricByAlias returns the first metric with alias, or nil.
func findMetricByAlias(payload *protobuf.Payload, alias uint64) *protobuf.Payload_Metric {
	for _, m := range payload.GetMetrics() {
		if m.GetAlias() == alias {
			return m
		}
	}
	return nil
}

// commandID returns the CommandId a DACK echoes from its DCMD.
func commandID(p *protobuf.Payload) string {
	return findMetric(p, commandIDMetricName).GetStringValue()
}
