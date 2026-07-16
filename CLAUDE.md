# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project Overview

Go client library for IOTech Edge Connect (XRT) Management APIs over MQTT. Module path: `github.com/IOTechSystems/go-mod-edge-connect-client/v4`.

## Build & Test Commands

```bash
make test          # Full suite: unit tests + lint + vet + gofmt check
make unittest      # Unit tests with coverage
make lint          # golangci-lint (x86_64 only)
make tidy          # go mod tidy
make vendor        # go mod vendor
```

CGO is disabled (`CGO_ENABLED=0`). Run a single test:
```bash
CGO_ENABLED=0 go test ./pkg/xrt/topicmgr/... -run TestFunctionName
```

## Architecture

```
pkg/interfaces/interface.go          EdgeClient interface (all public operations)
pkg/xrt/xrt.go                       Client struct + NewXrtClient constructor
pkg/xrt/common.go                    FetchXRTResponse / FetchXRTResWithSubTimeout
pkg/xrt/xrt{device,profile,schedule,component,discovery}.go  Domain method impls
pkg/xrt/topicmgr/
  topicmanager.go                    topicManager interface + topicManagerBase
  topicmanagerpool.go                TmPool singleton (ref-counted, shared subscriptions)
  replytopicmanager.go               Request/response routing via RequestMap
  dispatchertopicmanager.go          One-to-many broadcast (discovery, status topics)
  requestmap.go                      Thread-safe map: requestId → chan []byte
```

**Request/response flow:**
1. Build request struct → assign UUID `requestId`
2. `RequestMap.Add(requestId, bufSize)` — creates the reply channel **before** publishing
3. `messageBus.PublishBinaryData(...)` — send to XRT
4. `FetchXRTResponse` blocks on the channel until reply or timeout
5. Unmarshal → check `commonResponse.Result.Error()`

**`DiscoverComponents` / multi-reply flow** uses `sendXrtRequestWithSubTimeout` + `FetchXRTResWithSubTimeout`, which collects all responses until a subscribe timeout expires (not just the first reply).

**`TmPool`** is a package-level singleton. Multiple `Client` instances sharing the same MQTT topic get the same underlying subscription; `ReleaseTopicManager` decrements the ref count and only unsubscribes when it hits zero.

## Code Conventions

- All public methods return `errors.EdgeX`, never plain `error`.
- Wrap errors with `errors.NewCommonEdgeX(errors.Kind, msg, err)` or `errors.NewCommonEdgeXWrapper(err)`.
- `context.Context` is the first parameter everywhere and is threaded into subscriptions for cancellation.
- Shared state uses `sync.RWMutex`; take a snapshot under lock, release lock, then execute handlers.
- Panic recovery is deferred in all message handler goroutines (see `topicManagerBase.startListening`).
- Branch naming: `EDX-XXXX-branch`. Commit messages must reference the Jira ticket: `EDX-XXXX ...`.

## Key Invariants — Do Not Break

- **Add the RequestMap entry before publishing.** If you publish first, the reply can arrive before the channel exists and will be dropped silently.
- **Never call `messageBus.Disconnect()` inside `Client.Close()`.** The message bus may be shared; the caller that created it is responsible for disconnecting.
- **`TmPool` stores either `*ReplyTopicManager` or `*DispatcherTopicManager` per topic, never both.** Registering the wrong type on an existing topic returns `KindContractInvalid`.
- **`FetchXRTResWithSubTimeout` requires a pointer-to-slice as `response`.** It uses reflection and will return an error for any other type.

## Adding a New EdgeClient Method

1. Add the signature to `pkg/interfaces/interface.go`.
2. Implement in the appropriate domain file (e.g., `xrtdevice.go`) following the existing pattern:
   - Build request struct with `requestId = uuid.New().String()`
   - Call `sendXrtRequest` (standard) or `sendXrtCommandRequest` / `sendXrtDiscoveryRequest` as needed
   - Unmarshal the typed response and return the result
3. No new domain file needed unless the method belongs to a genuinely new resource type.
