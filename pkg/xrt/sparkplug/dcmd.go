// Copyright (C) 2026 IOTech Ltd

package sparkplug

import (
	"cmp"
	"context"
	goerrors "errors"
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

	// commandFailedMessage is shown on the UI for a failure with no specific message; the details go to the log.
	commandFailedMessage = "Command execution failed"
)

// qualityMessages are the UI messages for the Uncertain and Bad quality codes XRT devsdk sets (DEVSDK_QUALITY_*); any
// other such code gets commandFailedMessage.
var qualityMessages = map[uint32]string{
	0x40900000: "The device returned only its last usable value",                    // Uncertain_LastUsableValue
	0x40930000: "The sensor value may not be accurate",                              // Uncertain_SensorNotAccurate
	0x40940000: "The value exceeds the engineering unit limits",                     // Uncertain_EngineeringUnitsExceeded
	0x80310000: "Unable to communicate with the device",                             // Bad_NoCommunication
	0x808B0000: "The device reported a failure",                                     // Bad_DeviceFailure
	0x80890000: "The device or resource configuration does not allow this command",  // Bad_ConfigurationError
	0x80320000: "The device has not provided a value yet",                           // Bad_WaitingForInitialData
	0x803C0000: "The value is out of range",                                         // Bad_OutOfRange
	0x80740000: "The value type does not match the resource",                        // Bad_TypeMismatch
	0x80E30000: "Not written because another value in the same command was invalid", // Bad_DependentValueChanged
}

func (c *Client) ReadDeviceResources(ctx context.Context, node models.NodeKey, device string,
	resourceNames []string) (xrtmodels.MultiResourcesResult, errors.EdgeX) {
	if len(resourceNames) == 0 {
		return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeX(errors.KindContractInvalid, "no resource to read", nil)
	}
	// find metrics of the specified device
	deviceDefs, err := c.deviceMetrics(node, device)
	if err != nil {
		return xrtmodels.MultiResourcesResult{}, err
	}
	// match the metrics with resourceNames, and create read metric to send the request
	matchedMetricDefs, metrics := make([]models.MetricDef, 0, len(resourceNames)), make([]*protobuf.Payload_Metric, 0, len(resourceNames))
	for _, resourceName := range resourceNames {
		def, err := resourceMetricDef(deviceDefs, device, resourceName, common.ReadWrite_R)
		if err != nil {
			return xrtmodels.MultiResourcesResult{}, err
		}
		matchedMetricDefs, metrics = append(matchedMetricDefs, def), append(metrics, readMetric(def))
	}

	ack, acked, err := c.request(ctx, node, device, matchedMetricDefs, metrics)
	if err != nil {
		return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeXWrapper(err)
	}
	result := xrtmodels.MultiResourcesResult{Device: device, Readings: make(map[string]xrtmodels.Reading, len(matchedMetricDefs))}
	for _, def := range matchedMetricDefs {
		reading, err := newReading(def, acked[def.Name], ack)
		if err != nil {
			c.lc.Errorf("Sparkplug DCMD to %s/%s/%s: %s %v, which a reading cannot hold exactly",
				node.Group, node.Node, device, def.Name, err)
			return xrtmodels.MultiResourcesResult{}, errors.NewCommonEdgeX(errors.KindServerError,
				fmt.Sprintf("%s: The value is too large to be shown exactly", def.Name), nil)
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
	// find metrics of the specified device
	deviceMetricDefs, err := c.deviceMetrics(node, device)
	if err != nil {
		return err
	}
	// match the metrics with resourceNames, and create write metric to send the request
	matchedMetricDefs, metrics := make([]models.MetricDef, 0, len(resourceValuePairs)), make([]*protobuf.Payload_Metric, 0, len(resourceValuePairs))
	for _, resourceName := range slices.Sorted(maps.Keys(resourceValuePairs)) {
		def, err := resourceMetricDef(deviceMetricDefs, device, resourceName, common.ReadWrite_W)
		if err != nil {
			return err
		}
		m, err := writeMetric(def, resourceValuePairs[resourceName])
		if err != nil {
			return err
		}
		matchedMetricDefs, metrics = append(matchedMetricDefs, def), append(metrics, m)
	}

	if _, _, err := c.request(ctx, node, device, matchedMetricDefs, metrics); err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	return nil
}

// deviceMetrics returns the DBIRTH metrics of an online device; they are never modified in place (see NodeInfo).
func (c *Client) deviceMetrics(node models.NodeKey, device string) ([]models.MetricDef, errors.EdgeX) {
	c.nodesMu.RLock()
	defer c.nodesMu.RUnlock()

	nodeInfo, ok := c.nodes[node]
	if !ok {
		return nil, errors.NewCommonEdgeX(errors.KindEntityDoesNotExist, fmt.Sprintf("Sparkplug node %s/%s is not online", node.Group, node.Node), nil)
	}
	i := slices.IndexFunc(nodeInfo.Devices, func(d models.Device) bool { return d.Name == device })
	if i < 0 {
		return nil, errors.NewCommonEdgeX(errors.KindEntityDoesNotExist,
			fmt.Sprintf("device %s is not online on Sparkplug node %s/%s", device, node.Group, node.Node), nil)
	}
	return nodeInfo.Devices[i].Metrics, nil
}

// resourceMetricDef returns the DBIRTH definition of a resource, checked before sending since XRT reports no error for
// an unknown resource. access is common.ReadWrite_R or common.ReadWrite_W.
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
	// The DACK is matched by alias, so XRT could apply a write the caller is told has failed.
	if def.Alias == 0 {
		return models.MetricDef{}, errors.NewCommonEdgeX(errors.KindNotImplemented,
			fmt.Sprintf("resource %s has no alias in the DBIRTH, which DCMD needs to match its DACK", name), nil)
	}
	return def, nil
}

// request publishes a DCMD of metrics, one per def, and returns the DACK with its metric of each def by name. A missing DACK
// cannot be told apart from a slow one, and XRT may already have applied a write when it times out.
func (c *Client) request(ctx context.Context, node models.NodeKey, device string, defs []models.MetricDef,
	metrics []*protobuf.Payload_Metric) (*protobuf.Payload, map[string]*protobuf.Payload_Metric, errors.EdgeX) {
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
	ack := c.addDACKWaiter(id)
	defer c.removeDACKWaiter(id)

	topic := fmt.Sprintf("%s/%s/%s/%s/%s", namespace, node.Group, msgTypeDCMD, node.Node, device)
	// The payload is formatted only if debug logging is on.
	c.lc.Debugf("Publish Sparkplug DCMD to %s: %v", topic, dcmd)
	if err := c.messageBus.PublishBinaryData(data, topic); err != nil {
		return nil, nil, errors.NewCommonEdgeX(errors.KindCommunicationError, fmt.Sprintf("failed to publish DCMD to %s", topic), err)
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case p := <-ack:
		// handle the DACK
		acked, err := c.ackedMetrics(topic, p, defs)
		return p, acked, err
	case <-ctx.Done():
		return nil, nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, fmt.Sprintf("cancelled while waiting for the DACK of %s", topic), ctx.Err())
	case <-c.done:
		return nil, nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, "the Sparkplug client is closed", nil)
	case <-timer.C:
		return nil, nil, errors.NewCommonEdgeX(errors.KindServiceUnavailable, fmt.Sprintf("no DACK of %s within %v", topic, timeout), nil)
	}
}

// checkSendable rejects a DCMD that could not be answered, before XRT applies a write the caller is told has failed.
// A Close that races with the publish can still let one through.
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
	ack := make(chan *protobuf.Payload, 1)
	c.dackWaitersMu.Lock()
	c.dackWaiters[id] = ack
	c.dackWaitersMu.Unlock()
	return ack
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
	ack, ok := c.dackWaiters[id]
	delete(c.dackWaiters, id)
	c.dackWaitersMu.Unlock()
	if !ok {
		c.lc.Debugf("Ignore Sparkplug DACK on %s: no request is waiting for CommandId %q", topic, id)
		return
	}
	ack <- p
}

// newDCMD returns a DCMD carrying metrics. AckGood makes XRT put the value of each metric in the DACK, so the DACK
// alone answers the request.
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

// writeMetric encodes value as the DBIRTH datatype of def, which XRT does not check against the resource.
func writeMetric(def models.MetricDef, value any) (*protobuf.Payload_Metric, errors.EdgeX) {
	m := &protobuf.Payload_Metric{Name: proto.String(def.Name), Datatype: proto.Uint32(def.Datatype)}
	if err := setValue(m, protobuf.DataType(def.Datatype), value); err != nil {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid,
			fmt.Sprintf("invalid value %v for %s of type %s", value, def.Name, valueTypes[protobuf.DataType(def.Datatype)]), err)
	}
	return m, nil
}

// ackedMetrics returns the DACK metric of each def, matched by alias; a def missing from the DACK or failed fails the
// whole request.
func (c *Client) ackedMetrics(topic string, ack *protobuf.Payload, defs []models.MetricDef) (map[string]*protobuf.Payload_Metric, errors.EdgeX) {
	acked := make(map[string]*protobuf.Payload_Metric, len(defs))
	var failed, messages []string
	for _, def := range defs {
		resourceAck := findMetricByAlias(ack, def.Alias)
		if err := validateResourceAck(def, resourceAck); err != nil {
			failed = append(failed, fmt.Sprintf("%s (%v)", def.Name, err))
			messages = append(messages, fmt.Sprintf("%s: %s", def.Name, qualityMessage(resourceAck)))
			continue
		}
		acked[def.Name] = resourceAck
	}
	if len(failed) > 0 {
		slices.Sort(failed)
		slices.Sort(messages)
		c.lc.Errorf("Sparkplug DCMD to %s failed: %s", topic, strings.Join(failed, ", "))
		return nil, errors.NewCommonEdgeX(errors.KindServerError, strings.Join(messages, "; "), nil)
	}
	return acked, nil
}

// validateResourceAck reports why the DACK metric m of def failed, for the log; m is nil if the DACK lacks it.
func validateResourceAck(def models.MetricDef, m *protobuf.Payload_Metric) error {
	switch {
	case m == nil:
		return goerrors.New("missing from the DACK")
	case m.GetName() != "" && m.GetName() != def.Name:
		return fmt.Errorf("its alias belongs to %s in the DACK; the DBIRTH may be stale", m.GetName())
	}
	// Checked before is_null, which a failed metric may also carry.
	if q := quality(m); q&qualityNotGood != 0 {
		return fmt.Errorf("quality 0x%08X", q)
	}
	if m.GetIsNull() {
		return goerrors.New("no value")
	}
	if _, ok := readingValue(protobuf.DataType(def.Datatype), m); !ok {
		return fmt.Errorf("the value does not match the DBIRTH datatype %s", protobuf.DataType(def.Datatype))
	}
	return nil
}

// qualityMessage returns the error message for a failed metric; only a quality code has a specific one.
func qualityMessage(m *protobuf.Payload_Metric) string {
	return cmp.Or(qualityMessages[quality(m)], commandFailedMessage)
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
