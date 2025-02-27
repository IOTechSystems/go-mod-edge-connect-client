// Copyright (C) 2023-2024 IOTech Ltd

package xrt

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/IOTechSystems/go-mod-edge-connect-client/v4/pkg/interfaces"

	"github.com/IOTechSystems/go-mod-central-ext/v4/pkg/xrtmodels"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/clients/logger"
	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

const (
	clientName = "central-go-client"
)

// Client implements the client of MQTT management API, https://docs.iotechsys.com/edge-xrt21/mqtt-management/mqtt-management.html
type Client struct {
	lc              logger.LoggingClient
	requestMap      RequestMap
	messageBus      messaging.MessageClient
	requestTopic    string
	replyTopic      string
	responseTimeout time.Duration

	clientOptions *ClientOptions
}

type MessageHandler func(message types.MessageEnvelope)

type Subscription struct {
	topicChannel   types.TopicChannel
	messageHandler MessageHandler
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
	DiscoveryMessageHandler MessageHandler
}

// DiscoveryOptions provides the config for sending the discovery request like discovery:trigger, device:scan
type DiscoveryOptions struct {
	DiscoveryTopic           string
	DiscoveryMessageHandler  MessageHandler
	DiscoveryDuration        time.Duration
	DiscoveryTimeout         time.Duration
	ExtentedDiscoveryOptions map[string]any
}

// StatusOptions provides the config for subscribing the XRT status
type StatusOptions struct {
	StatusTopic          string
	StatusMessageHandler MessageHandler
}

func NewXrtClient(ctx context.Context, messageBus messaging.MessageClient, requestTopic string, replyTopic string,
	responseTimeout time.Duration, lc logger.LoggingClient, clientOptions *ClientOptions) (interfaces.EdgeClient, errors.EdgeX) {
	client := &Client{
		lc:              lc,
		requestMap:      NewRequestMap(),
		messageBus:      messageBus,
		requestTopic:    requestTopic,
		replyTopic:      replyTopic,
		responseTimeout: responseTimeout,
		clientOptions:   clientOptions,
	}

	err := initSubscriptions(ctx, client, clientOptions, lc)
	if err != nil {
		return client, errors.NewCommonEdgeX(errors.Kind(err), "failed to init subscriptions", err)
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

func NewCommandOptions(commandTopic string, discoveryTopic string, discoveryMessageHandler MessageHandler) *CommandOptions {
	return &CommandOptions{
		CommandTopic:            commandTopic,
		DiscoveryTopic:          discoveryTopic,
		DiscoveryMessageHandler: discoveryMessageHandler,
	}
}

func NewDiscoveryOptions(discoveryTopic string, discoveryMessageHandler MessageHandler, discoveryDuration, discoveryTimeout time.Duration) *DiscoveryOptions {
	return NewDiscoveryOptionsExtended(discoveryTopic, discoveryMessageHandler, discoveryDuration, discoveryTimeout, make(map[string]any))
}

func NewDiscoveryOptionsExtended(discoveryTopic string, discoveryMessageHandler MessageHandler, discoveryDuration, discoveryTimeout time.Duration, extendedDiscoveryOptions map[string]any) *DiscoveryOptions {
	return &DiscoveryOptions{
		DiscoveryTopic:           discoveryTopic,
		DiscoveryMessageHandler:  discoveryMessageHandler,
		DiscoveryDuration:        discoveryDuration,
		DiscoveryTimeout:         discoveryTimeout,
		ExtentedDiscoveryOptions: extendedDiscoveryOptions,
	}
}

func NewStatusOptions(statusTopic string, statusMessageHandler MessageHandler) *StatusOptions {
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
	timeout := time.Duration(c.responseTimeout.Nanoseconds() + c.clientOptions.DiscoveryDuration.Nanoseconds() + c.clientOptions.DiscoveryTimeout.Nanoseconds())
	return c.sendXrtRequestWithTimeout(ctx, c.requestTopic, requestId, request, response, timeout)
}

// sendXrtCommandRequest sends command request to XRT
func (c *Client) sendXrtCommandRequest(ctx context.Context, requestId string, request interface{}, response interface{}) errors.EdgeX {
	if c.clientOptions == nil || c.clientOptions.CommandOptions == nil {
		return errors.NewCommonEdgeX(errors.KindContractInvalid, "please provide CommandOptions for the command request", nil)
	}
	return c.sendXrtRequestWithTimeout(ctx, c.clientOptions.CommandOptions.CommandTopic, requestId, request, response, c.responseTimeout)
}

func (c *Client) sendXrtRequestWithTimeout(ctx context.Context, requestTopic string, requestId string, request interface{}, response interface{}, responseTimeout time.Duration) errors.EdgeX {
	jsonData, err := json.Marshal(request)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}

	// Before publishing the request, we should create responseChan to receive the response from XRT
	c.requestMap.Add(requestId)

	err = c.messageBus.PublishBinaryData(jsonData, requestTopic)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to send the XRT request", err)
	}

	cmdResponseBytes, err := FetchXRTResponse(ctx, requestId, c.requestMap, responseTimeout)
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
	jsonData, err := json.Marshal(request)
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}

	// Before publishing the request, we should create responseChan to receive the response from XRT
	c.requestMap.Add(requestId)

	err = c.messageBus.PublishBinaryData(jsonData, requestTopic)
	if err != nil {
		return errors.NewCommonEdgeX(errors.Kind(err), "failed to send the XRT request", err)
	}

	edgexErr := FetchXRTResWithSubTimeout(ctx, requestId, c.requestMap, subscribeTimeout, response)
	if edgexErr != nil {
		return errors.NewCommonEdgeXWrapper(edgexErr)
	}

	return nil
}

func initSubscriptions(ctx context.Context, xrtClient *Client, clientOptions *ClientOptions, lc logger.LoggingClient) errors.EdgeX {
	subscriptions := createSubscriptions(xrtClient, clientOptions)
	messageErrors := make(chan error)
	// Create goroutine to handle MessageBus errors
	go func() {
		for {
			select {
			case <-ctx.Done():
				lc.Info("Exiting waiting for MessageBus errors")
				return
			case msgErr := <-messageErrors:
				lc.Errorf("error receiving message from bus, %s", msgErr.Error())
			}
		}
	}()
	// Create goroutines to receive message for each subscription
	for _, sub := range subscriptions {
		go func(subscription Subscription) {
			lc.Infof("Waiting for messages from the MessageBus on the '%s' topic", subscription.topicChannel.Topic)
			for {
				select {
				case <-ctx.Done():
					lc.Infof("Exiting waiting for MessageBus '%s' topic messages", subscription.topicChannel.Topic)

					// invoke the Unsubscribe method before exiting waiting for specific topic
					err := xrtClient.messageBus.Unsubscribe(subscription.topicChannel.Topic)
					if err != nil {
						lc.Errorf("Error occurred while unsubscribing from topic %s", subscription.topicChannel.Topic)
					}
					return
				case message := <-subscription.topicChannel.Messages:
					lc.Debugf("Received message from the topic %s", subscription.topicChannel.Topic)
					subscription.messageHandler(message)
				}
			}
		}(sub)
		err := xrtClient.messageBus.SubscribeBinaryData([]types.TopicChannel{sub.topicChannel}, messageErrors)
		if err != nil {
			return errors.NewCommonEdgeX(errors.Kind(err), fmt.Sprintf("subscribe to topic '%s' failed", sub.topicChannel.Topic), err)
		}
		lc.Debugf("Subscribed to %s", sub.topicChannel.Topic)
	}
	return nil
}

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

func createSubscriptions(xrtClient *Client, clientOptions *ClientOptions) []Subscription {
	var subscriptions []Subscription
	if xrtClient.replyTopic != "" {
		subscriptions = append(subscriptions, subscription(xrtClient.replyTopic, commandReplyHandler(xrtClient.requestMap, xrtClient.lc)))
	}

	if clientOptions == nil {
		return subscriptions
	}
	if clientOptions.CommandOptions != nil {
		if clientOptions.CommandOptions.DiscoveryTopic != "" && clientOptions.CommandOptions.DiscoveryMessageHandler != nil {
			subscriptions = append(subscriptions, subscription(clientOptions.CommandOptions.DiscoveryTopic, clientOptions.CommandOptions.DiscoveryMessageHandler))
		}
	}
	if clientOptions.DiscoveryOptions != nil {
		if clientOptions.DiscoveryOptions.DiscoveryTopic != "" && clientOptions.DiscoveryOptions.DiscoveryMessageHandler != nil {
			subscriptions = append(subscriptions, subscription(clientOptions.DiscoveryOptions.DiscoveryTopic, clientOptions.DiscoveryOptions.DiscoveryMessageHandler))
		}
	}
	if clientOptions.StatusOptions != nil {
		if clientOptions.StatusOptions.StatusTopic != "" && clientOptions.StatusOptions.StatusMessageHandler != nil {
			subscriptions = append(subscriptions, subscription(clientOptions.StatusOptions.StatusTopic, clientOptions.StatusOptions.StatusMessageHandler))
		}
	}
	return subscriptions
}

func subscription(topic string, messageHandler MessageHandler) Subscription {
	return Subscription{
		topicChannel: types.TopicChannel{
			Topic:    topic,
			Messages: make(chan types.MessageEnvelope),
		},
		messageHandler: messageHandler,
	}
}

func (c *Client) Close() errors.EdgeX {
	err := c.messageBus.Disconnect()
	if err != nil {
		return errors.NewCommonEdgeXWrapper(err)
	}
	return nil
}
