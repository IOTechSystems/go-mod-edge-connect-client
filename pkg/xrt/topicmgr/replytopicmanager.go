// Copyright (C) 2026 IOTech Ltd

package topicmgr

import (
	"context"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

func commandReplyHandler(requestMap RequestMap, lc logger.LoggingClient) MessageHandler {
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
		resChan, ok := requestMap.Get(response.RequestId)
		if !ok {
			lc.Debugf("deprecated response from the XRT, it might be caused by timeout or unknown error, topic: %s, message:%s", message.ReceivedTopic, message.Payload)
			return
		}

		resChan <- message.Payload.([]byte)
	}
}

// ReplyTopicManager manages a reply topic with a shared RequestMap for request/response matching
type ReplyTopicManager struct {
	topicManagerBase
	RequestMap RequestMap
}

func newReplyTopicManager(topic string, messageBus messaging.MessageClient, lc logger.LoggingClient, cancelFunc context.CancelFunc) *ReplyTopicManager {
	return &ReplyTopicManager{
		topicManagerBase: newTopicManagerBase(topic, messageBus, lc, cancelFunc),
		RequestMap:       NewRequestMap(),
	}
}

func (rtm *ReplyTopicManager) subscribe(subscriptionCtx context.Context) errors.EdgeX {
	handler := commandReplyHandler(rtm.RequestMap, rtm.lc)
	return rtm.startListening(subscriptionCtx, handler)
}
