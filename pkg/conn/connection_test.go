// Copyright (C) 2026 IOTech Ltd

package conn

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/edgexfoundry/go-mod-core-contracts/v4/errors"
	"github.com/edgexfoundry/go-mod-messaging/v4/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func broker(authMode string) BrokerSettings {
	return BrokerSettings{
		Type:     "mqtt",
		Address:  "tcp://broker:1883",
		AuthMode: authMode,
		Options: map[string]string{
			OptionQos: "1", OptionKeepAlive: "10", OptionRetained: "true", OptionAutoReconnect: "true",
			OptionConnectTimeout: "5s", OptionSkipCertVerify: "false",
		},
		Username: "user", Password: "pass", CaCert: "ca", ClientCert: "cert", ClientKey: "key",
	}
}

// Expected maps follow connect-ui's former CreateEdgeInstMsgClient and setSecretsToOptionalMap line by line; when
// connect-ui switched over, a one-off probe compared the two over 560 inputs and found only the port change.
func TestMessageBusConfig_AuthModes(t *testing.T) {
	base := map[string]string{
		"ClientId": "connect-mcp", "Qos": "1", "KeepAlive": "10", "Retained": "true", "AutoReconnect": "true",
		"ConnectTimeout": "5", "SkipCertVerify": "false",
	}
	with := func(extra map[string]string) map[string]string {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	tests := []struct {
		authMode string
		want     map[string]string
	}{
		{AuthModeUsernamePassword, with(map[string]string{"Username": "user", "Password": "pass", "CaPEMBlock": "ca"})},
		{AuthModeCert, with(map[string]string{"CertPEMBlock": "cert", "KeyPEMBlock": "key", "CaPEMBlock": "ca"})},
		{AuthModeCA, with(map[string]string{"CaPEMBlock": "ca"})},
	}
	for _, tt := range tests {
		t.Run(tt.authMode, func(t *testing.T) {
			got, err := MessageBusConfig(broker(tt.authMode), "connect-mcp")
			require.NoError(t, err)
			assert.Equal(t, types.MessageBusConfig{
				Broker:   types.HostInfo{Host: "broker", Port: 1883, Protocol: "tcp"},
				Type:     "mqtt",
				Optional: tt.want,
			}, got)
		})
	}
}

// In mode none connect-ui reads no secrets, so the settings carry none and no secret key reaches the map.
func TestMessageBusConfig_NoSecrets(t *testing.T) {
	b := broker(AuthModeNone)
	b.Username, b.Password, b.CaCert, b.ClientCert, b.ClientKey = "", "", "", "", ""
	got, err := MessageBusConfig(b, "id")
	require.NoError(t, err)
	for _, key := range []string{"Username", "Password", "CertPEMBlock", "KeyPEMBlock", "CaPEMBlock"} {
		assert.NotContains(t, got.Optional, key)
	}
}

func TestMessageBusConfig_ClientIDFromCaller(t *testing.T) {
	b := broker(AuthModeNone)
	b.Options[OptionClientID] = "connect-ui"
	got, err := MessageBusConfig(b, "connect-mcp")
	require.NoError(t, err)
	assert.Equal(t, "connect-mcp", got.Optional[OptionClientID])
}

func TestMessageBusConfig_LenientOptions(t *testing.T) {
	b := broker(AuthModeNone)
	b.Options = map[string]string{OptionConnectTimeout: "5s", OptionQos: "x", OptionRetained: "maybe"}
	got, err := MessageBusConfig(b, "id")
	require.NoError(t, err)
	assert.Equal(t, "0", got.Optional[OptionQos])
	assert.Equal(t, "0", got.Optional[OptionKeepAlive])
	assert.Equal(t, "false", got.Optional[OptionRetained])
	assert.Equal(t, "false", got.Optional[OptionAutoReconnect])
}

func TestMessageBusConfig_Errors(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*BrokerSettings)
		kind    errors.ErrKind
		message string
	}{
		{"connect timeout missing", func(b *BrokerSettings) { delete(b.Options, OptionConnectTimeout) },
			errors.KindContractInvalid, "invalid ConnectTimeout '' from broker"},
		{"connect timeout without unit", func(b *BrokerSettings) { b.Options[OptionConnectTimeout] = "5" },
			errors.KindContractInvalid, "invalid ConnectTimeout '5' from broker"},
		{"address not a URL", func(b *BrokerSettings) { b.Address = "tcp://broker:1883/%zz" },
			errors.KindServerError, "failed to parse the address from broker"},
		{"no port", func(b *BrokerSettings) { b.Address = "tcp://broker" },
			errors.KindServerError, "failed to parse the port from broker"},
		{"port above 65535", func(b *BrokerSettings) { b.Address = "tcp://broker:65536" },
			errors.KindServerError, "failed to parse the port from broker"},
		{"negative port", func(b *BrokerSettings) { b.Address = "tcp://broker:-1" },
			errors.KindServerError, "failed to parse the address from broker"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := broker(AuthModeNone)
			tt.modify(&b)
			_, err := MessageBusConfig(b, "id")
			require.Error(t, err)
			assert.Equal(t, tt.kind, errors.Kind(err))
			assert.Contains(t, err.Error(), tt.message)
		})
	}
}

// Measured before the fix: the parse error quoted the whole address, password included, into logs and tool errors.
func TestMessageBusConfig_AddressErrorOmitsCredentials(t *testing.T) {
	for _, address := range []string{"tcp://user:secret@broker:18x3", "tcp://user:secret@broker:1883/%zz"} {
		b := broker(AuthModeNone)
		b.Address = address
		_, err := MessageBusConfig(b, "id")
		require.Error(t, err, address)
		assert.Contains(t, err.Error(), "failed to parse the address from broker", address)
		assert.NotContains(t, err.Error(), "secret", address)
	}
}

// connect-ui parsed the port as a signed 16-bit number and refused 32768–65535.
func TestMessageBusConfig_Ports(t *testing.T) {
	for _, port := range []int{32767, 32768, 65535} {
		b := broker(AuthModeNone)
		b.Address = "ssl://broker:" + strconv.Itoa(port)
		got, err := MessageBusConfig(b, "id")
		require.NoError(t, err, port)
		assert.Equal(t, types.HostInfo{Host: "broker", Port: port, Protocol: "ssl"}, got.Broker)
	}
}

func TestNewMessageClient(t *testing.T) {
	client, err := NewMessageClient(broker(AuthModeNone), "id")
	require.NoError(t, err)
	assert.NotNil(t, client)

	b := broker(AuthModeNone)
	b.Type = "unknown"
	_, err = NewMessageClient(b, "id")
	require.Error(t, err)
	assert.Equal(t, errors.KindServerError, errors.Kind(err))
}

// The JSON field names are the contract between the services that exchange EdgeConnectionSettings.
func TestEdgeConnectionSettings_JSON(t *testing.T) {
	s := EdgeConnectionSettings{
		Broker:          BrokerSettings{Type: "mqtt", Address: "tcp://b:1883", AuthMode: AuthModeNone, Options: map[string]string{}},
		SparkplugGroups: []string{"iotech"},
		RequestTimeout:  "10s",
		MaxNodeCount:    1024,
	}
	data, err := json.Marshal(s)
	require.NoError(t, err)
	assert.JSONEq(t, `{"broker":{"type":"mqtt","address":"tcp://b:1883","authMode":"none","options":{}},
		"sparkplugGroups":["iotech"],"requestTimeout":"10s","maxNodeCount":1024}`, string(data))
}
