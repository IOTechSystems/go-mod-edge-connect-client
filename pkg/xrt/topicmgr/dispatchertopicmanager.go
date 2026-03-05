// Copyright (C) 2026 IOTech Ltd

package topicmgr

import (
	"context"
	"reflect"
	"slices"
	"sync"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// DispatcherTopicManager manages discovery/status topics by dispatching messages to multiple registered handlers
type DispatcherTopicManager struct {
	topicManagerBase
	mutex    sync.RWMutex
	handlers []MessageHandler
}

func newDispatcherTopicManager(topic string, messageBus messaging.MessageClient, lc logger.LoggingClient, cancelFunc context.CancelFunc) *DispatcherTopicManager {
	return &DispatcherTopicManager{
		topicManagerBase: newTopicManagerBase(topic, messageBus, lc, cancelFunc),
		handlers:         make([]MessageHandler, 0),
	}
}

func (dtm *DispatcherTopicManager) subscribe(subscriptionCtx context.Context) errors.EdgeX {
	handler := dtm.createMessageDispatcher()
	return dtm.startListening(subscriptionCtx, handler)
}

// RegisterHandler registers a handler to DispatcherTopicManager
func (dtm *DispatcherTopicManager) RegisterHandler(handler MessageHandler) {
	dtm.mutex.Lock()
	defer dtm.mutex.Unlock()
	dtm.handlers = append(dtm.handlers, handler)
}

// UnregisterHandler unregisters a handler from DispatcherTopicManager
func (dtm *DispatcherTopicManager) UnregisterHandler(handler MessageHandler) {
	if handler == nil {
		return
	}
	dtm.mutex.Lock()
	defer dtm.mutex.Unlock()

	// Use reflect to compare function pointers
	handlerPtr := reflect.ValueOf(handler).Pointer()
	for i, h := range dtm.handlers {
		if h != nil && reflect.ValueOf(h).Pointer() == handlerPtr {
			dtm.handlers = slices.Delete(dtm.handlers, i, i+1)
			break
		}
	}
}

// createMessageDispatcher creates a dispatcher handler that distributes messages to all registered handlers
func (dtm *DispatcherTopicManager) createMessageDispatcher() MessageHandler {
	return func(message types.MessageEnvelope) {
		// Copy handlers list to avoid holding lock during handler execution.
		// This allows RegisterHandler/UnregisterHandler to proceed concurrently
		// without blocking the message dispatch loop, and ensures a consistent
		// snapshot of handlers is used for the current message.
		dtm.mutex.RLock()
		handlers := make([]MessageHandler, len(dtm.handlers))
		copy(handlers, dtm.handlers)
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
