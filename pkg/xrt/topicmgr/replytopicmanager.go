// Copyright (C) 2026 IOTech Ltd

package topicmgr

import (
	"context"
	"fmt"
	"sync"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

func (rtm *ReplyTopicManager) commandReplyHandler() MessageHandler {
	requestMap, lc := rtm.RequestMap, rtm.lc
	return func(message types.MessageEnvelope) {
		err := message.ConvertMsgPayloadToByteArray()
		if err != nil {
			lc.Errorf("failed to convert message payload to byte array: %v", err)
			return
		}
		var response xrtmodels.BaseResponse
		response, err = types.GetMsgPayload[xrtmodels.BaseResponse](message)
		if err != nil {
			lc.Warnf("failed to parse XRT reply, message:%s, err: %v", message.Payload, err)
			return
		}
		if response.Type == xrtmodels.MessageTypeDeviceDiscovery {
			rtm.handleDiscovery(message)
			return
		}
		resChan, ok := requestMap.Get(response.RequestId)
		if !ok {
			lc.Debugf("deprecated response from the XRT, it might be caused by timeout or unknown error, topic: %s, message:%s", message.ReceivedTopic, message.Payload)
			return
		}

		select {
		case resChan <- message.Payload.([]byte):
		default:
			lc.Debugf("dropping XRT reply because reply channel is not ready (no receiver waiting or buffer full), requestId: %s, topic: %s", response.RequestId, message.ReceivedTopic)
		}
	}
}

// ReplyTopicManager manages a reply topic with a shared RequestMap for request/response matching.
// XRT 3.4 also publishes device discovery results on the reply topic, without a request_id; they go to the handler set
// by SetDiscoveryHandler instead.
type ReplyTopicManager struct {
	topicManagerBase
	RequestMap RequestMap

	discoveryMutex   sync.Mutex
	discoveryHandler MessageHandler
}

func newReplyTopicManager(topic string, messageBus messaging.MessageClient, lc logger.LoggingClient, cancelFunc context.CancelFunc) *ReplyTopicManager {
	return &ReplyTopicManager{
		topicManagerBase: newTopicManagerBase(topic, messageBus, lc, cancelFunc),
		RequestMap:       NewRequestMap(),
	}
}

func (rtm *ReplyTopicManager) subscribe(subscriptionCtx context.Context) errors.EdgeX {
	handler := rtm.commandReplyHandler()
	return rtm.startListening(subscriptionCtx, handler)
}

// SetDiscoveryHandler sets the handler for the device discovery results on this reply topic. A reply topic belongs to
// one device service, which runs one discovery at a time, so a second handler is rejected until ClearDiscoveryHandler.
func (rtm *ReplyTopicManager) SetDiscoveryHandler(handler MessageHandler) errors.EdgeX {
	if handler == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "handler must not be nil", nil)
	}
	rtm.discoveryMutex.Lock()
	defer rtm.discoveryMutex.Unlock()
	if rtm.discoveryHandler != nil {
		return errors.NewCommonEdgeX(errors.KindStatusConflict,
			fmt.Sprintf("topic '%s' already has a discovery handler", rtm.Topic), nil)
	}
	rtm.discoveryHandler = handler
	return nil
}

// ClearDiscoveryHandler removes the handler set by SetDiscoveryHandler.
func (rtm *ReplyTopicManager) ClearDiscoveryHandler() {
	rtm.discoveryMutex.Lock()
	defer rtm.discoveryMutex.Unlock()
	rtm.discoveryHandler = nil
}

// handleDiscovery runs the discovery handler on its own goroutine, so a slow handler never blocks the replies.
func (rtm *ReplyTopicManager) handleDiscovery(message types.MessageEnvelope) {
	rtm.discoveryMutex.Lock()
	handler := rtm.discoveryHandler
	rtm.discoveryMutex.Unlock()
	if handler == nil {
		rtm.lc.Debugf("dropping XRT device discovery result with no discovery running, topic: %s", message.ReceivedTopic)
		return
	}
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				rtm.lc.Errorf("panic in the discovery handler for topic %s: %v", rtm.Topic, recovered)
			}
		}()
		handler(message)
	}()
}
