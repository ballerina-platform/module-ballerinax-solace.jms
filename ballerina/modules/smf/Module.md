## Overview

The `ballerinax/solace.smf` module provides an SMF-native API for the Solace PubSub+ event broker,
built on the Solace PubSub+ Messaging API for Java. It complements the JMS-based API in the
`ballerinax/solace` module and unlocks capabilities the JMS abstraction hides: per-publisher and
per-receiver quality of service, publish receipts with back-pressure control, negative message
settlement, message replay, shared subscriptions, and native request-reply.

Use this module for these SMF-native capabilities; use the `ballerinax/solace` module when you
need JMS semantics such as transacted sessions, durable topic subscribers, or `MapMessage`
(structured SDT map) payloads.

## Publishers

The module provides two publishers with per-publisher quality of service:

- `smf:DirectPublisher` - direct (at-most-once) delivery for high-throughput scenarios which can
  tolerate message loss.
- `smf:PersistentPublisher` - guaranteed (persistent) delivery; each publish call blocks until the
  broker acknowledges the message (publish receipt). Back-pressure is configurable via the
  `backPressure` field (`WAIT_WHEN_FULL`, `REJECT_WHEN_FULL`, or `ELASTIC`).

```ballerina
import ballerinax/solace.smf;

public function main() returns error? {
    smf:PersistentPublisher publisher = check new ("smf://localhost:55555",
        auth = {username: "admin", password: "admin"}
    );
    check publisher->publish("Hello, World!", "orders/retail/usa");
    check publisher->close();
}
```

To deliver persistent messages to a queue, add a topic subscription to the queue on the broker
(or use the receiver's `topicSubscriptions` field) and publish to the subscribed topic.

## Receivers

- `smf:DirectReceiver` - receives direct messages from topic subscriptions. Set the `shareName`
  field to form a shared subscription where multiple receivers consume from the same topics in a
  load-balanced manner.
- `smf:PersistentReceiver` - receives guaranteed messages from a durable queue. Supports
  programmatic topic-to-queue mapping (`topicSubscriptions`), message selectors, endpoint
  provisioning (`missingResourcesStrategy: smf:CREATE_ON_START`), message replay, and per-message
  settlement.

```ballerina
smf:PersistentReceiver receiver = check new ("smf://localhost:55555",
    auth = {username: "admin", password: "admin"},
    queueName = "orders"
);
smf:Message? message = check receiver->receive(10);
if message is smf:Message {
    check receiver->ack(message);
}
```

### Message settlement

By default (`autoAck: false`), received messages must be settled explicitly. `ack()` settles with
the `ACCEPTED` outcome. With `negativeSettlementEnabled: true` (requires broker 10.2.1+), two
negative outcomes become available: `failed()` increments the delivery count and triggers
redelivery (routing to a dead message queue once the queue's max-redelivery limit is exceeded),
and `rejected()` removes the message without redelivery. Unsettled messages are redelivered when
the receiver flow reconnects, so messages are never silently lost.

### Message replay

Set the `replayStrategy` field on the persistent receiver to replay messages from the broker's
replay log: `smf:ALL_MESSAGES`, `{fromTime: <time:Utc>}`, or `{afterMessageId: <replication group
message id>}`. Replay requires a replay log on the broker and is not supported with partitioned
queues or under replication.

## Services

The `smf:Listener` delivers messages to attached `smf:Service` instances using the messaging
API's native asynchronous delivery:

```ballerina
listener smf:Listener smfListener = check new ("smf://localhost:55555",
    auth = {username: "admin", password: "admin"}
);

@smf:ServiceConfig {
    queueName: "orders"
}
service on smfListener {
    remote function onMessage(smf:Message message, smf:Caller caller) returns error? {
        // process the message
        check caller->ack(message);
    }
}
```

Annotate the service with a `queueName` (persistent, settlement via `smf:Caller`) or
`topicSubscriptions` (direct, optionally shared via `shareName`). The `smf:Caller` supports
`ack()`, `failed()`, and `rejected()`.

## Request-Reply

`smf:Requester` and `smf:Replier` implement the native request-reply pattern:

```ballerina
// Requester side
smf:Requester requester = check new ("smf://localhost:55555", auth = {...});
smf:Message reply = check requester->request("ping", "service/echo");

// Replier side
smf:Replier replier = check new ("smf://localhost:55555", auth = {...},
    topicSubscription = "service/echo");
smf:Message? request = check replier->receive(10);
if request is smf:Message {
    check replier->reply(request, "pong");
}
```

## Examples

The Solace connector provides practical examples illustrating usage in various scenarios. Explore
these [examples](https://github.com/ballerina-platform/module-ballerinax-solace/tree/main/examples/).

1. [Order processing (SMF-native)](https://github.com/ballerina-platform/module-ballerinax-solace/tree/main/examples/order-processing/) - Demonstrates guaranteed publish with publish receipts, data binding, explicit acknowledgement, negative settlement (`REJECTED`) of a poison message, and native request-reply.
