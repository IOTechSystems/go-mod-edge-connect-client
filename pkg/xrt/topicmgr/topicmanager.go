// Copyright (C) 2026 IOTech Ltd

package topicmgr

import (
	"context"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// MessageHandler defines a callback function for processing messages received from the message bus
type MessageHandler func(message types.MessageEnvelope)

// topicManager defines the interface for managing a shared topic subscription.
// It is used by TopicManagerPool to manage reference counting and lifecycle.
type topicManager interface {
	incrementRefCount()
	decrementRefCount() int
	shutdown()
}

// topicManagerBase provides common fields and functionality shared by all topic managers
type topicManagerBase struct {
	Topic         string
	messageBus    messaging.MessageClient
	lc            logger.LoggingClient
	cancelFunc    context.CancelFunc
	messageErrors chan error
	topicChannel  types.TopicChannel
	refCount      int
}

func newTopicManagerBase(topic string, messageBus messaging.MessageClient, lc logger.LoggingClient, cancelFunc context.CancelFunc) topicManagerBase {
	return topicManagerBase{
		Topic:         topic,
		messageBus:    messageBus,
		lc:            lc,
		cancelFunc:    cancelFunc,
		messageErrors: make(chan error),
		topicChannel: types.TopicChannel{
			Topic:    topic,
			Messages: make(chan types.MessageEnvelope),
		},
		refCount: 1,
	}
}

func (b *topicManagerBase) incrementRefCount() {
	b.refCount++
}

func (b *topicManagerBase) decrementRefCount() int {
	b.refCount--
	return b.refCount
}

func (b *topicManagerBase) shutdown() {
	b.cancelFunc()
	err := b.messageBus.Unsubscribe(b.Topic)
	if err != nil {
		b.lc.Errorf("failed to unsubscribe from topic '%s': %v", b.Topic, err)
	} else {
		b.lc.Debugf("Unsubscribed from topic '%s'", b.Topic)
	}
}

// startListening starts the goroutine that listens for messages and subscribes to the topic
func (b *topicManagerBase) startListening(subscriptionCtx context.Context, handler MessageHandler) errors.EdgeX {
	go func() {
		b.lc.Infof("Waiting for messages from the MessageBus on the '%s' topic", b.topicChannel.Topic)
		for {
			select {
			case <-subscriptionCtx.Done():
				b.lc.Infof("Exiting waiting for MessageBus '%s' topic messages", b.topicChannel.Topic)
				return
			case msgErr := <-b.messageErrors:
				b.lc.Errorf("error receiving message from bus, %s", msgErr.Error())
			case message := <-b.topicChannel.Messages:
				b.lc.Debugf("Received message from the topic %s", b.topicChannel.Topic)
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							b.lc.Errorf("panic while handling message from topic '%s': %v", b.topicChannel.Topic, recovered)
						}
					}()
					handler(message)
				}()
			}
		}
	}()

	err := b.messageBus.SubscribeBinaryData([]types.TopicChannel{b.topicChannel}, b.messageErrors)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to subscribe to topic", err)
	}

	b.lc.Debugf("Subscribed to %s", b.topicChannel.Topic)
	return nil
}
