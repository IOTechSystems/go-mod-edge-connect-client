// Copyright (C) 2023-2026 IOTech Ltd

package xrt

import (
	"context"
	"encoding/json"
	"time"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/interfaces"
	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/xrt/topicmgr"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
)

const (
	clientName = "central-go-client"
)

// Client implements the client of MQTT management API, https://docs.iotechsys.com/edge-xrt21/mqtt-management/mqtt-management.html
type Client struct {
	lc              logger.LoggingClient
	messageBus      messaging.MessageClient
	requestTopic    string
	replyTopic      string
	responseTimeout time.Duration

	clientOptions *ClientOptions

	replyTopicManager            *topicmgr.ReplyTopicManager
	commandDiscoveryTopicManager *topicmgr.DispatcherTopicManager
	commandDiscoveryHandlerID    topicmgr.HandlerID
	discoveryTopicManager        *topicmgr.DispatcherTopicManager
	discoveryHandlerID           topicmgr.HandlerID
	statusTopicManager           *topicmgr.DispatcherTopicManager
	statusHandlerID              topicmgr.HandlerID
}

type ClientOptions struct {
	*CommandOptions
	*DiscoveryOptions
	*StatusOptions
}

// CommandOptions provides the config for sending the request to manage components
type CommandOptions struct {
	CommandTopic            string
	DiscoveryTopic          string
	DiscoveryMessageHandler topicmgr.MessageHandler
}

// DiscoveryOptions provides the config for sending the discovery request like discovery:trigger, device:scan
type DiscoveryOptions struct {
	DiscoveryTopic           string
	DiscoveryMessageHandler  topicmgr.MessageHandler
	DiscoveryTimeout         time.Duration
	ExtendedDiscoveryOptions map[string]any
	// MaxNodeCount is the maximum number of XRT nodes allowed to reply to a discovery request.
	// It sets the response channel buffer size to avoid dropping replies when multiple nodes respond concurrently.
	// Defaults to 1024 if not set.
	MaxNodeCount uint
}

// StatusOptions provides the config for subscribing the XRT status
type StatusOptions struct {
	StatusTopic          string
	StatusMessageHandler topicmgr.MessageHandler
}

func NewXrtClient(_ context.Context, messageBus messaging.MessageClient, requestTopic string, replyTopic string,
	responseTimeout time.Duration, lc logger.LoggingClient, clientOptions *ClientOptions) (interfaces.EdgeClient, errors.EdgeX) {

	client := &Client{
		lc:              lc,
		messageBus:      messageBus,
		requestTopic:    requestTopic,
		replyTopic:      replyTopic,
		responseTimeout: responseTimeout,
		clientOptions:   clientOptions,
	}

	// Initialize ReplyTopic subscription
	if replyTopic != "" {
		var err errors.EdgeX
		replyManager, err := topicmgr.TmPool.GetReplyTopicManager(replyTopic, messageBus, lc)
		if err != nil {
			return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to init subscriptions", err)
		}
		client.replyTopicManager = replyManager
	}

	// Initialize subscriptions for other topics defined in clientOptions
	if clientOptions != nil {
		if err := client.initCommandDiscoverySubscription(clientOptions, lc); err != nil {
			_ = client.Close()
			return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to init command discovery subscription", err)
		}
		if err := client.initDiscoverySubscription(clientOptions, lc); err != nil {
			_ = client.Close()
			return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to init discovery subscription", err)
		}
		if err := client.initStatusSubscription(clientOptions, lc); err != nil {
			_ = client.Close()
			return nil, errors.NewCommonEdgeX(errors.Kind(err), "failed to init status subscription", err)
		}
	}

	return client, nil
}

func NewClientOptions(commandOptions *CommandOptions, discoveryOptions *DiscoveryOptions, statusOptions *StatusOptions) *ClientOptions {
	return &ClientOptions{
		CommandOptions:   commandOptions,
		DiscoveryOptions: discoveryOptions,
		StatusOptions:    statusOptions,
	}
}

func NewCommandOptions(commandTopic string, discoveryTopic string, discoveryMessageHandler topicmgr.MessageHandler) *CommandOptions {
	return &CommandOptions{
		CommandTopic:            commandTopic,
		DiscoveryTopic:          discoveryTopic,
		DiscoveryMessageHandler: discoveryMessageHandler,
	}
}

func NewDiscoveryOptions(discoveryTopic string, discoveryMessageHandler topicmgr.MessageHandler, discoveryTimeout time.Duration, extendedDiscoveryOptions map[string]any, maxNodeCount uint) *DiscoveryOptions {
	if maxNodeCount == 0 {
		maxNodeCount = 1024
	}
	return &DiscoveryOptions{
		DiscoveryTopic:           discoveryTopic,
		DiscoveryMessageHandler:  discoveryMessageHandler,
		DiscoveryTimeout:         discoveryTimeout,
		ExtendedDiscoveryOptions: extendedDiscoveryOptions,
		MaxNodeCount:             maxNodeCount,
	}
}

func NewStatusOptions(statusTopic string, statusMessageHandler topicmgr.MessageHandler) *StatusOptions {
	return &StatusOptions{
		StatusTopic:          statusTopic,
		StatusMessageHandler: statusMessageHandler,
	}
}

func (c *Client) SetResponseTimeout(responseTimeout time.Duration) {
	c.responseTimeout = responseTimeout
}

// sendXrtRequest sends general request to XRT
func (c *Client) sendXrtRequest(ctx context.Context, requestId string, request interface{}, response interface{}) errors.EdgeX {
	return c.sendXrtRequestWithTimeout(ctx, c.requestTopic, requestId, request, response, c.responseTimeout)
}

// sendXrtDiscoveryRequest sends discovery request to XRT
func (c *Client) sendXrtDiscoveryRequest(ctx context.Context, requestId string, request interface{}, response interface{}) errors.EdgeX {
	if c.clientOptions == nil || c.clientOptions.DiscoveryOptions == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "please provide DiscoveryOptions for the discovery request", nil)
	}
	timeout := time.Duration(c.responseTimeout.Nanoseconds() + c.clientOptions.DiscoveryTimeout.Nanoseconds())
	return c.sendXrtRequestWithTimeout(ctx, c.requestTopic, requestId, request, response, timeout)
}

// sendXrtCommandRequest sends command request to XRT
func (c *Client) sendXrtCommandRequest(ctx context.Context, requestId string, request interface{}, response interface{}) errors.EdgeX {
	if c.clientOptions == nil || c.clientOptions.CommandOptions == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "please provide CommandOptions for the command request", nil)
	}
	return c.sendXrtRequestWithTimeout(ctx, c.clientOptions.CommandTopic, requestId, request, response, c.responseTimeout)
}

func (c *Client) sendXrtRequestWithTimeout(ctx context.Context, requestTopic string, requestId string, request interface{}, response interface{}, responseTimeout time.Duration) errors.EdgeX {
	if c.replyTopicManager == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "replyTopic is required for sending XRT request", nil)
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}

	// Before publishing the request, we should create responseChan to receive the response from XRT
	c.replyTopicManager.RequestMap.Add(requestId, 1)

	err = c.messageBus.PublishBinaryData(jsonData, requestTopic)
	if err != nil {
		c.replyTopicManager.RequestMap.Delete(requestId)
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to send the XRT request", err)
	}

	cmdResponseBytes, err := FetchXRTResponse(ctx, requestId, c.replyTopicManager.RequestMap, responseTimeout)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}

	err = json.Unmarshal(cmdResponseBytes, response)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to JSON decoding command response: %v", err)
	}

	// handle error result from the XRT
	var commonResponse xrtmodels.CommonResponse
	err = json.Unmarshal(cmdResponseBytes, &commonResponse)
	if err != nil {
		return errors.NewCommonEdgeX(errors.KindServerError, "failed to JSON decoding command response: %v", err)
	}
	if commonResponse.Result.Error() != nil {
		return errors.NewCommonEdgeXWrapper(commonResponse.Result.Error())
	}
	return nil
}

// sendXrtRequestWithSubTimeout publish the xrt request and wait for responses from multiple xrt nodes for the specific subscribe timeout
func (c *Client) sendXrtRequestWithSubTimeout(ctx context.Context, requestTopic string, requestId string, request any,
	response any, subscribeTimeout time.Duration) errors.EdgeX {
	if c.replyTopicManager == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "replyTopic is required for sending XRT request", nil)
	}

	jsonData, err := json.Marshal(request)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}

	// Before publishing the request, we should create responseChan to receive the response from XRT.
	// Use MaxNodeCount as buffer capacity so replies from multiple XRT nodes don't get dropped.
	c.replyTopicManager.RequestMap.Add(requestId, c.clientOptions.MaxNodeCount)

	err = c.messageBus.PublishBinaryData(jsonData, requestTopic)
	if err != nil {
		c.replyTopicManager.RequestMap.Delete(requestId)
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to send the XRT request", err)
	}

	edgexErr := FetchXRTResWithSubTimeout(ctx, requestId, c.replyTopicManager.RequestMap, subscribeTimeout, response)
	if edgexErr != nil {
		return errors.NewCommonEdgeXWrapper(edgexErr)
	}

	return nil
}

// initCommandDiscoverySubscription initializes the CommandOptions.DiscoveryTopic subscription
func (c *Client) initCommandDiscoverySubscription(clientOptions *ClientOptions, lc logger.LoggingClient) errors.EdgeX {
	if clientOptions.CommandOptions == nil || clientOptions.CommandOptions.DiscoveryTopic == "" ||
		clientOptions.CommandOptions.DiscoveryMessageHandler == nil {
		return nil
	}
	manager, err := topicmgr.TmPool.GetDispatcherTopicManager(clientOptions.CommandOptions.DiscoveryTopic, c.messageBus, lc)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	handlerID, err := manager.RegisterHandler(clientOptions.CommandOptions.DiscoveryMessageHandler)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	c.commandDiscoveryHandlerID = handlerID
	c.commandDiscoveryTopicManager = manager
	return nil
}

// initDiscoverySubscription initializes the DiscoveryOptions.DiscoveryTopic subscription
func (c *Client) initDiscoverySubscription(clientOptions *ClientOptions, lc logger.LoggingClient) errors.EdgeX {
	if clientOptions.DiscoveryOptions == nil || clientOptions.DiscoveryOptions.DiscoveryTopic == "" ||
		clientOptions.DiscoveryOptions.DiscoveryMessageHandler == nil {
		return nil
	}
	manager, err := topicmgr.TmPool.GetDispatcherTopicManager(clientOptions.DiscoveryOptions.DiscoveryTopic, c.messageBus, lc)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	handlerID, err := manager.RegisterHandler(clientOptions.DiscoveryOptions.DiscoveryMessageHandler)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	c.discoveryHandlerID = handlerID
	c.discoveryTopicManager = manager
	return nil
}

// initStatusSubscription initializes the StatusOptions.StatusTopic subscription
func (c *Client) initStatusSubscription(clientOptions *ClientOptions, lc logger.LoggingClient) errors.EdgeX {
	if clientOptions.StatusOptions == nil || clientOptions.StatusTopic == "" ||
		clientOptions.StatusMessageHandler == nil {
		return nil
	}
	manager, err := topicmgr.TmPool.GetDispatcherTopicManager(clientOptions.StatusTopic, c.messageBus, lc)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	handlerID, err := manager.RegisterHandler(clientOptions.StatusMessageHandler)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	c.statusHandlerID = handlerID
	c.statusTopicManager = manager
	return nil
}

func (c *Client) Close() errors.EdgeX {
	// Note: We don't call c.messageBus.Disconnect() here because the messageBus client may be used by other xrt clients.
	// The disconnect should be handled by the code that created the messageBus client.

	if c.replyTopicManager != nil {
		topicmgr.TmPool.ReleaseTopicManager(c.replyTopic)
		c.replyTopicManager = nil
	}

	if c.commandDiscoveryTopicManager != nil {
		c.commandDiscoveryTopicManager.UnregisterHandler(c.commandDiscoveryHandlerID)
		topicmgr.TmPool.ReleaseTopicManager(c.commandDiscoveryTopicManager.Topic)
		c.commandDiscoveryTopicManager = nil
	}

	if c.discoveryTopicManager != nil {
		c.discoveryTopicManager.UnregisterHandler(c.discoveryHandlerID)
		topicmgr.TmPool.ReleaseTopicManager(c.discoveryTopicManager.Topic)
		c.discoveryTopicManager = nil
	}

	if c.statusTopicManager != nil {
		c.statusTopicManager.UnregisterHandler(c.statusHandlerID)
		topicmgr.TmPool.ReleaseTopicManager(c.statusTopicManager.Topic)
		c.statusTopicManager = nil
	}
	return nil
}
