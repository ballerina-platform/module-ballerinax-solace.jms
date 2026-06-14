# Specification: Ballerina `solace.smf` Module

_Authors_: @PasinduGunarathne \
_Reviewers_: \
_Created_: 2026/06/12 \
_Updated_: 2026/06/12 \
_Edition_: Swan Lake

## Introduction

This is the specification for the `solace.smf` module of the [Ballerina language](https://ballerina.io),
which provides an SMF-native API for the Solace PubSub+ event broker, built on the Solace PubSub+
Messaging API for Java. It coexists with the JMS-based API specified in [spec.md](./spec.md): the
JMS surface keeps its names and semantics, and both modules ship in the same `ballerinax/solace`
package.

The `solace.smf` module exposes capabilities the JMS abstraction hides:

- Per-publisher and per-receiver quality of service (direct vs persistent)
- Publish receipts with configurable publisher back-pressure
- Per-message settlement outcomes: `ACCEPTED`, `FAILED`, `REJECTED` (negative acknowledgement)
- Message replay from the broker replay log
- Shared subscriptions for load-balanced direct consumers
- Programmatic topic-to-queue mapping and missing-endpoint provisioning
- Native request-reply

## Contents

1. [Connection configuration](#1-connection-configuration)
2. [Message](#2-message)
3. [Publishers](#3-publishers)
4. [Receivers](#4-receivers)
5. [Listener and services](#5-listener-and-services)
6. [Request-reply](#6-request-reply)

## 1. Connection configuration

All SMF clients and the listener accept a broker URL and an `smf:ConnectionConfiguration`.
URL schemes `smf://` and `smfs://` (as used by the JMS surface) are accepted and translated to the
messaging API's `tcp://`/`tcps://` schemes; multiple hosts may be given as a comma-separated list
for failover.

```ballerina
public type ConnectionConfiguration record {
    string messageVpn = "default";
    solace:BasicAuthConfig|solace:KerberosConfig|solace:OAuth2Config auth?;
    SecureSocket secureSocket?;
    solace:RetryConfig retryConfig?;
    string clientName?;
    string applicationDescription?;
    decimal connectTimeout = 30.0;
    int compressionLevel = 0;
};
```

The authentication records (`BasicAuthConfig`, `KerberosConfig`, `OAuth2Config`), the
`TrustStore`/`KeyStore` records, and `RetryConfig` are shared with the `ballerinax/solace` module.
Configuring `secureSocket.keyStore` without an explicit `auth` enables client certificate
authentication. The SMF-specific `smf:SecureSocket` uses the excluded-protocols TLS model
(`excludedProtocols`) instead of the JMS surface's enabled-protocols list, and does not support
the deprecated trusted-common-names validation.

## 2. Message

```ballerina
public type Message record {|
    anydata payload;
    string correlationId?;
    map<string> properties?;
    string applicationMessageId?;
    string applicationMessageType?;
    int priority?;
    decimal timeToLive?;
    boolean dmqEligible = true;
    int sequenceNumber?;
    string senderId?;
    // Broker-set fields (inbound only)
    boolean redelivered?;
    string destinationName?;
    string replicationGroupMessageId?;
    int expiration?;
    int timestamp?;
    int senderTimestamp?;
|};
```

Outbound payload conversion: `string` and `xml` payloads are sent as text, `byte[]` as bytes, and
all other `anydata` values as JSON-encoded bytes. The underlying messaging API supports
string-typed user properties only; structured SDT map payloads remain a JMS-surface capability.
Receive operations support data binding to subtypes of `smf:Message` with typed payload fields.

## 3. Publishers

### 3.1. `smf:DirectPublisher`

Publishes with direct (at-most-once) delivery. The topic is supplied per publish call.

```ballerina
public isolated client class DirectPublisher {
    public isolated function init(string url, *PublisherConfiguration config) returns Error?;
    isolated remote function publish(anydata|Message message, string topic) returns Error?;
    isolated remote function close() returns Error?;
}
```

### 3.2. `smf:PersistentPublisher`

Publishes with guaranteed delivery. Each `publish` call blocks until the broker acknowledges the
message (publish receipt) or the timeout elapses.

```ballerina
public isolated client class PersistentPublisher {
    public isolated function init(string url, *PublisherConfiguration config) returns Error?;
    isolated remote function publish(anydata|Message message, string topic, decimal timeout = 30.0)
        returns Error?;
    isolated remote function close() returns Error?;
}
```

### 3.3. Back-pressure

Both publishers accept a back-pressure configuration:

```ballerina
public type BackPressureConfig record {|
    BackPressureStrategy strategy = ELASTIC;  // WAIT_WHEN_FULL | REJECT_WHEN_FULL | ELASTIC
    int bufferCapacity = 1024;                // ignored for ELASTIC
|};
```

## 4. Receivers

### 4.1. `smf:DirectReceiver`

Receives direct messages from one or more topic subscriptions. Setting `shareName` forms a shared
subscription: multiple receivers with the same share name receive matching messages in a
load-balanced manner, each message delivered to exactly one member of the group.

```ballerina
public isolated client class DirectReceiver {
    public isolated function init(string url, *DirectReceiverConfiguration config) returns Error?;
    isolated remote function receive(decimal timeout = 10.0, typedesc<Message> T = <>) returns T|Error?;
    isolated remote function close() returns Error?;
}
```

### 4.2. `smf:PersistentReceiver`

Receives guaranteed messages from a durable queue.

```ballerina
public type PersistentReceiverConfiguration record {
    *ConnectionConfiguration;
    string queueName;
    ReplayStrategy replayStrategy?;
    string[] topicSubscriptions = [];           // programmatic topic-to-queue mapping
    string messageSelector?;
    MissingResourcesStrategy missingResourcesStrategy = DO_NOT_CREATE;
    boolean autoAck = false;
    boolean negativeSettlementEnabled = false;
};

public isolated client class PersistentReceiver {
    public isolated function init(string url, *PersistentReceiverConfiguration config) returns Error?;
    isolated remote function receive(decimal timeout = 10.0, typedesc<Message> T = <>) returns T|Error?;
    isolated remote function ack(Message message) returns Error?;
    isolated remote function failed(Message message) returns Error?;
    isolated remote function rejected(Message message) returns Error?;
    isolated remote function pause() returns Error?;
    isolated remote function 'resume() returns Error?;
    isolated remote function close() returns Error?;
}
```

### 4.3. Message settlement

By default (`autoAck: false`) messages must be settled explicitly; unsettled messages are
redelivered after the receiver flow reconnects, so messages are never silently lost.

- `ack()` settles with the `ACCEPTED` outcome and removes the message from the queue.
- `failed()` settles with the `FAILED` outcome: the broker increments the delivery count and
  redelivers; once the queue's max-redelivery limit is exceeded the message is moved to the dead
  message queue, if configured.
- `rejected()` settles with the `REJECTED` outcome: the message is removed without redelivery.

The negative outcomes require `negativeSettlementEnabled: true` (broker 10.2.1 or later) and are
mutually exclusive with `autoAck`.

### 4.4. Missing-resource provisioning

`missingResourcesStrategy: CREATE_ON_START` provisions the queue (and its topic subscriptions) on
the broker when it does not exist. This is the SMF counterpart of the JMS surface's
`enableDynamicDurables`. Provisioned queues use the exclusive access type.

### 4.5. Message replay

```ballerina
public type ReplayStrategy ALL_MESSAGES|TimeBasedReplay|ReplicationGroupIdReplay;

public type TimeBasedReplay record {| time:Utc fromTime; |};
public type ReplicationGroupIdReplay record {| string afterMessageId; |};
```

When `replayStrategy` is set, the broker redelivers eligible messages from the replay log to the
receiver before live messages. Replay requires a replay log to be provisioned on the broker; a
time-based replay start time must not precede the replay log's creation time. Replay is not
supported with partitioned queues or under replication.

## 5. Listener and services

The `smf:Listener` delivers messages to attached `smf:Service` instances using the messaging
API's native asynchronous delivery (no polling). Each service gets a dedicated dispatch thread,
which keeps message processing ordered per service.

```ballerina
public type ServiceConfiguration DirectSubscriptionConfig|QueueSubscriptionConfig;

public type DirectSubscriptionConfig record {|
    string[] topicSubscriptions;
    string shareName?;
|};

public type QueueSubscriptionConfig record {|
    string queueName;
    string[] topicSubscriptions = [];
    string messageSelector?;
    MissingResourcesStrategy missingResourcesStrategy = DO_NOT_CREATE;
    boolean autoAck = false;
    boolean negativeSettlementEnabled = false;
|};
```

A service declares an `onMessage` remote method taking an `smf:Message` (or a subtype) and
optionally an `smf:Caller`, plus an optional `onError` method taking an `smf:Error`. The
`smf:Caller` provides `ack()`, `failed()`, and `rejected()` and is only available for queue
(persistent) subscriptions; declaring it on a direct subscription service is rejected at attach
time. With `autoAck: true`, messages are acknowledged automatically after `onMessage` returns
successfully.

```ballerina
@smf:ServiceConfig {
    queueName: "orders"
}
service on smfListener {
    remote function onMessage(smf:Message message, smf:Caller caller) returns error? {
        check caller->ack(message);
    }
}
```

## 6. Request-reply

`smf:Requester` and `smf:Replier` implement the native request-reply pattern with broker-managed
reply-to topics and correlation.

```ballerina
public isolated client class Requester {
    public isolated function init(string url, *ConnectionConfiguration config) returns Error?;
    isolated remote function request(anydata|Message message, string topic, decimal replyTimeout = 30.0)
        returns Message|Error;
    isolated remote function close() returns Error?;
}

public isolated client class Replier {
    public isolated function init(string url, *ReplierConfiguration config) returns Error?;
    isolated remote function receive(decimal timeout = 10.0, typedesc<Message> T = <>) returns T|Error?;
    isolated remote function reply(Message request, anydata|Message response) returns Error?;
    isolated remote function close() returns Error?;
}
```

The replier subscribes to a request topic (optionally shared via `shareName` for load-balanced
repliers). A received request message carries its reply handle internally; passing it to `reply`
sends the correlated response to the requester.
