// Copyright (C) 2026 IOTech Ltd

package topicmgr

import (
	"context"
	"fmt"
	"sync"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
)

// TopicManagerPool manages shared topic subscriptions using reference counting.
// It stores topicManager interface values, which are either *ReplyTopicManager or *DispatcherTopicManager.
type TopicManagerPool struct {
	managers map[string]topicManager // key: topic
	mutex    sync.RWMutex
}

var TmPool = &TopicManagerPool{
	managers: make(map[string]topicManager),
}

// GetReplyTopicManager returns an existing ReplyTopicManager for the given topic, or creates a new one.
func (pool *TopicManagerPool) GetReplyTopicManager(
	topic string,
	messageBus messaging.MessageClient,
	lc logger.LoggingClient) (*ReplyTopicManager, errors.EdgeX) {

	if topic == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "topic cannot be empty", nil)
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if existing, ok := pool.managers[topic]; ok {
		rtm, ok := existing.(*ReplyTopicManager)
		if !ok {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid,
				fmt.Sprintf("topic '%s' is already subscribed with a different topic manager", topic), nil)
		}
		existing.incrementRefCount()
		return rtm, nil
	}

	subscriptionCtx, cancelFunc := context.WithCancel(context.Background())
	manager := newReplyTopicManager(topic, messageBus, lc, cancelFunc)

	if err := manager.subscribe(subscriptionCtx); err != nil {
		cancelFunc()
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to subscribe to topic", err)
	}

	pool.managers[topic] = manager
	return manager, nil
}

// GetDispatcherTopicManager returns an existing DispatcherTopicManager for the given topic, or creates a new one.
func (pool *TopicManagerPool) GetDispatcherTopicManager(
	topic string,
	messageBus messaging.MessageClient,
	lc logger.LoggingClient) (*DispatcherTopicManager, errors.EdgeX) {

	if topic == "" {
		return nil, errors.NewCommonEdgeX(errors.KindContractInvalid, "topic cannot be empty", nil)
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	if existing, ok := pool.managers[topic]; ok {
		dtm, ok := existing.(*DispatcherTopicManager)
		if !ok {
			return nil, errors.NewCommonEdgeX(errors.KindContractInvalid,
				fmt.Sprintf("topic '%s' is already subscribed with a different topic manager", topic), nil)
		}
		existing.incrementRefCount()
		return dtm, nil
	}

	subscriptionCtx, cancelFunc := context.WithCancel(context.Background())
	manager := newDispatcherTopicManager(topic, messageBus, lc, cancelFunc)

	if err := manager.subscribe(subscriptionCtx); err != nil {
		cancelFunc()
		return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to subscribe to topic", err)
	}

	pool.managers[topic] = manager
	return manager, nil
}

// ReleaseTopicManager decreases the reference count and removes the manager if no clients are using it
func (pool *TopicManagerPool) ReleaseTopicManager(topic string) {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()

	manager, ok := pool.managers[topic]
	if !ok {
		return
	}

	if manager.decrementRefCount() <= 0 {
		manager.shutdown()
		delete(pool.managers, topic)
	}
}
