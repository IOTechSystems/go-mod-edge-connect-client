// Copyright (C) 2026 IOTech Ltd

// Package conn holds the settings a service needs to reach edge instances, and builds the message client from
// them, so every service reads the settings the same way.
package conn

import (
	goErrors "errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
)

// Broker authentication modes.
const (
	AuthModeNone             = "none"
	AuthModeUsernamePassword = "usernamepassword"
	AuthModeCert             = "clientcert"
	AuthModeCA               = "cacert"
)

// Keys of BrokerSettings.Options, and of the go-mod-messaging Optional map.
const (
	OptionClientID       = "ClientId"
	OptionQos            = "Qos"
	OptionRetained       = "Retained"
	OptionAutoReconnect  = "AutoReconnect"
	OptionSkipCertVerify = "SkipCertVerify"
	OptionConnectTimeout = "ConnectTimeout"
	OptionKeepAlive      = "KeepAlive"
	optionUsername       = "Username"
	optionPassword       = "Password"
	optionCertPEMBlock   = "CertPEMBlock"
	optionKeyPEMBlock    = "KeyPEMBlock"
	optionCaPEMBlock     = "CaPEMBlock"
)

// ApiEdgeConnectionRoute is connect-ui's endpoint that serves EdgeConnectionSettings. It has no credential, so only the
// services' own network may reach it.
const ApiEdgeConnectionRoute = "/api/v1/internal/edge-connection"

// EdgeConnectionSettings is what a service needs to reach edge instances that one service (connect-ui) owns and may
// change. A field is only ever added, never renamed or removed: services built against different versions exchange it.
type EdgeConnectionSettings struct {
	Broker          BrokerSettings `json:"broker"`
	SparkplugGroups []string       `json:"sparkplugGroups"`
	// RequestTimeout is the XRT request timeout, a Go duration such as "10s".
	RequestTimeout string `json:"requestTimeout"`
	MaxNodeCount   uint   `json:"maxNodeCount"`
}

// BrokerSettings is the broker of the edge instances, with its secrets.
type BrokerSettings struct {
	Type     string `json:"type"`
	Address  string `json:"address"` // URL, e.g. "tcp://localhost:1883"
	AuthMode string `json:"authMode"`
	// Options are keyed by the Option* constants and hold no client id: each service brings its own (see
	// MessageBusConfig).
	Options    map[string]string `json:"options"`
	Username   string            `json:"username,omitempty"`
	Password   string            `json:"password,omitempty"`
	CaCert     string            `json:"caCert,omitempty"`
	ClientCert string            `json:"clientCert,omitempty"`
	ClientKey  string            `json:"clientKey,omitempty"`
}

// NewMessageClient builds the message client for the broker; clientID must be unique on the broker. The client is not
// connected yet.
func NewMessageClient(b BrokerSettings, clientID string) (messaging.MessageClient, errors.EdgeX) {
	config, edgexErr := MessageBusConfig(b, clientID)
	if edgexErr != nil {
		return nil, edgexErr
	}
	client, err := messaging.NewMessageClient(config)
	if err != nil {
		return nil, errors.NewCommonEdgeX(errors.KindServerError, "failed to parse the MessageClient for broker", err)
	}
	return client, nil
}

// MessageBusConfig converts the broker settings into the go-mod-messaging configuration NewMessageClient uses.
func MessageBusConfig(b BrokerSettings, clientID string) (types.MessageBusConfig, errors.EdgeX) {
	connectTimeout, err := time.ParseDuration(b.Options[OptionConnectTimeout])
	if err != nil {
		return types.MessageBusConfig{}, errors.NewCommonEdgeX(errors.KindContractInvalid,
			fmt.Sprintf("invalid ConnectTimeout '%s' from broker", b.Options[OptionConnectTimeout]), err)
	}

	optional := map[string]string{
		OptionClientID:       clientID,
		OptionQos:            strconv.Itoa(intOption(b.Options, OptionQos)),
		OptionKeepAlive:      strconv.Itoa(intOption(b.Options, OptionKeepAlive)),
		OptionRetained:       strconv.FormatBool(boolOption(b.Options, OptionRetained)),
		OptionAutoReconnect:  strconv.FormatBool(boolOption(b.Options, OptionAutoReconnect)),
		OptionConnectTimeout: strconv.Itoa(int(connectTimeout.Seconds())),
		OptionSkipCertVerify: strconv.FormatBool(boolOption(b.Options, OptionSkipCertVerify)),
	}
	addSecrets(optional, b)

	brokerURL, err := url.Parse(b.Address)
	if err != nil {
		// Without the url.Error wrapper, which quotes the whole address and so any credentials in it.
		var urlErr *url.Error
		if goErrors.As(err, &urlErr) {
			err = urlErr.Err
		}
		return types.MessageBusConfig{}, errors.NewCommonEdgeX(errors.KindServerError,
			"failed to parse the address from broker", err)
	}
	port, err := strconv.ParseUint(brokerURL.Port(), 10, 16)
	if err != nil {
		return types.MessageBusConfig{}, errors.NewCommonEdgeX(errors.KindServerError,
			"failed to parse the port from broker", err)
	}

	return types.MessageBusConfig{
		Broker:   types.HostInfo{Host: brokerURL.Hostname(), Port: int(port), Protocol: brokerURL.Scheme},
		Type:     b.Type,
		Optional: optional,
	}, nil
}

// addSecrets puts the secrets the auth mode uses into the go-mod-messaging Optional map; a CA certificate is added in
// every mode.
func addSecrets(optional map[string]string, b BrokerSettings) {
	switch b.AuthMode {
	case AuthModeUsernamePassword:
		optional[optionUsername] = b.Username
		optional[optionPassword] = b.Password
	case AuthModeCert:
		optional[optionCertPEMBlock] = b.ClientCert
		optional[optionKeyPEMBlock] = b.ClientKey
	}
	if b.CaCert != "" {
		optional[optionCaPEMBlock] = b.CaCert
	}
}

// intOption and boolOption read an option as connect-ui did: a missing or malformed value is the zero value.
func intOption(options map[string]string, key string) int {
	v, _ := strconv.Atoi(options[key])
	return v
}

func boolOption(options map[string]string, key string) bool {
	v, _ := strconv.ParseBool(options[key])
	return v
}
