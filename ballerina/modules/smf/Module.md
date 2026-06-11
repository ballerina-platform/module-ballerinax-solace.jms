## Overview

The `ballerinax/solace.smf` module provides an SMF-native API for the Solace PubSub+ event broker,
built on the Solace PubSub+ Messaging API for Java. It complements the JMS-based API in the
`ballerinax/solace` module and unlocks capabilities the JMS abstraction hides, such as
per-publisher quality of service and publish receipts with back-pressure control.

Use this module when you need SMF-native capabilities; use the `ballerinax/solace` module when you
need JMS semantics such as transacted sessions, message selectors, or durable topic subscribers.

## Publishers

The module provides two publishers with per-publisher quality of service:

- `smf:DirectPublisher` - direct (at-most-once) delivery for high-throughput scenarios which can
  tolerate message loss.
- `smf:PersistentPublisher` - guaranteed (persistent) delivery; each publish call blocks until the
  broker acknowledges the message.

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

To deliver persistent messages to a queue, add a topic subscription to the queue on the broker and
publish to the subscribed topic.
