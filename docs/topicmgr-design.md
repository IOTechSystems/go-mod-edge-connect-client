# Topic Manager (`topicmgr`) Design

## Problem

Multiple xrt Client instances may subscribe to the same MQTT topic.
Without coordination, this leads to duplicate subscriptions on the message bus and handlers overriding each other.
The `topicmgr` package solves this with **shared subscriptions**.

## Architecture Overview

```
TopicManagerPool (singleton, thread-safe)
  - map[topic] -> ReplyTopicManager       (request/response pattern)
  - map[topic] -> DispatcherTopicManager   (one-to-many broadcast pattern)
```

## Base Layer: `topicManagerBase`

The common foundation embedded by all managers. Responsible for:

- Spawning a listening goroutine via `startListening()` to receive messages from the message bus
- Managing the subscription context lifecycle (created from `context.Background()`, decoupled from any individual client)
- Reference counting (`refCount`) for shared lifecycle management
- `shutdown()` to cancel the context and unsubscribe from the topic

## Two Manager Types

| | ReplyTopicManager | DispatcherTopicManager |
|---|---|---|
| **Purpose** | Request/response matching | Multi-handler broadcast |
| **Use case** | Send XRT commands and wait for replies | Receive discovery/status messages |
| **Core structure** | `RequestMap` (requestId -> chan []byte) | `handlerMap` (HandlerID -> MessageHandler) |
| **Message flow** | Receive reply -> parse requestId -> send to matching channel | Receive message -> snapshot handlers -> dispatch to each via goroutine |

## Pool Management (`TopicManagerPool`)

The pool is a package-level singleton (`TmPool`) that manages all topic subscriptions.

### Acquiring a Manager

```
GetReplyTopicManager("topic/reply", messageBus, logger)
  - topic exists   -> refCount++ -> return existing manager
  - topic is new   -> create manager -> subscribe -> store in map
```

`GetDispatcherTopicManager` follows the same pattern. If a topic is already registered with a different manager type, an error is returned.

### Releasing a Manager

```
ReleaseTopicManager("topic/reply")
  - refCount-- > 0  -> no-op (other clients still using it)
  - refCount-- <= 0  -> shutdown() -> remove from map
```

## RequestMap (used by ReplyTopicManager)

A thread-safe map from request ID to response channel:

| Method | Description |
|---|---|
| `Add(requestId, capacity)` | Create a buffered `chan []byte` with the given capacity and store it in the map. Capacity of 1 is used for single-reply requests; a larger value (e.g. `MaxNodeCount`) is used for multi-node discovery requests to avoid dropping concurrent replies. |
| `Get(requestId)` | Retrieve the channel (used by the reply handler to send the response) |
| `Delete(requestId)` | Remove the entry from the map |

## Message Flow Examples

### Request/Response Flow (ReplyTopicManager)

```
Client.sendXrtRequest()
  -> RequestMap.Add(id, capacity)  // create buffered response channel
  -> messageBus.Publish(request)   // send the request
  -> FetchXRTResponse()            // select: channel / timeout / ctx.Done
         ^
         | resChan <- payload
         |
  commandReplyHandler()            // shared subscription receives reply
    -> parse requestId from response
    -> RequestMap.Get(id) -> obtain channel -> send payload
```

### Broadcast Flow (DispatcherTopicManager)

```
Message arrives on message bus
  -> createMessageDispatcher() handles it
    -> RLock, snapshot all handlers
    -> For each handler: spawn goroutine with recover()
```
