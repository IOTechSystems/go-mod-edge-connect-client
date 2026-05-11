// Copyright (C) 2026 IOTech Ltd

package topicmgr

import (
	"context"
	"sync"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// HandlerID is an opaque identifier returned by RegisterHandler, used to unregister the handler later.
type HandlerID uint64

// DispatcherTopicManager manages discovery/status topics by dispatching messages to multiple registered handlers
type DispatcherTopicManager struct {
	topicManagerBase
	mutex      sync.RWMutex
	handlerMap map[HandlerID]MessageHandler
	nextID     HandlerID
}

func newDispatcherTopicManager(topic string, messageBus messaging.MessageClient, lc logger.LoggingClient, cancelFunc context.CancelFunc) *DispatcherTopicManager {
	return &DispatcherTopicManager{
		topicManagerBase: newTopicManagerBase(topic, messageBus, lc, cancelFunc),
		handlerMap:       make(map[HandlerID]MessageHandler),
	}
}

func (dtm *DispatcherTopicManager) subscribe(subscriptionCtx context.Context) errors.EdgeX {
	handler := dtm.createMessageDispatcher()
	return dtm.startListening(subscriptionCtx, handler)
}

// RegisterHandler registers a handler to DispatcherTopicManager and returns a HandlerID for later unregistration.
func (dtm *DispatcherTopicManager) RegisterHandler(handler MessageHandler) (HandlerID, errors.EdgeX) {
	if handler == nil {
		return 0, errors.NewCommonEdgeX(errors.KindContractInvalid, "handler must not be nil", nil)
	}
	dtm.mutex.Lock()
	defer dtm.mutex.Unlock()
	dtm.nextID++
	id := dtm.nextID
	dtm.handlerMap[id] = handler
	return id, nil
}

// UnregisterHandler unregisters a handler by its HandlerID from DispatcherTopicManager.
func (dtm *DispatcherTopicManager) UnregisterHandler(id HandlerID) {
	dtm.mutex.Lock()
	defer dtm.mutex.Unlock()
	delete(dtm.handlerMap, id)
}

// createMessageDispatcher creates a dispatcher handler that distributes messages to all registered handlers
func (dtm *DispatcherTopicManager) createMessageDispatcher() MessageHandler {
	return func(message types.MessageEnvelope) {
		// Copy handlers list to avoid holding lock during handler execution.
		// This allows RegisterHandler/UnregisterHandler to proceed concurrently
		// without blocking the message dispatch loop, and ensures a consistent
		// snapshot of handlers is used for the current message.
		dtm.mutex.RLock()
		handlers := make([]MessageHandler, 0, len(dtm.handlerMap))
		for _, h := range dtm.handlerMap {
			handlers = append(handlers, h)
		}
		dtm.mutex.RUnlock()

		// Distribute message to all registered handlers asynchronously
		for _, handler := range handlers {
			go func(h MessageHandler) {
				defer func() {
					if r := recover(); r != nil {
						dtm.lc.Errorf("panic in handler for topic %s: %v", dtm.Topic, r)
					}
				}()
				h(message)
			}(handler)
		}
	}
}
