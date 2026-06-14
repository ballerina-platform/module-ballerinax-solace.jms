## Overview

Solace PubSub+ is a powerful event broker that supports multiple protocols and messaging patterns. It provides high-performance, reliable, and scalable messaging for modern event-driven architectures. The Solace connector allows you to integrate with Solace event brokers, enabling efficient event distribution across various environments.

The package provides two complementary API surfaces:

- **`ballerinax/solace`** (this module) - a JMS-based API with `MessageProducer`, `MessageConsumer`, and a `Listener`, providing standard JMS semantics such as transacted sessions, message selectors, and durable topic subscribers.
- **`ballerinax/solace.smf`** - an SMF-native API built on the Solace PubSub+ Messaging API for Java, providing per-publisher/per-receiver quality of service, publish receipts with back-pressure control, negative message settlement (`FAILED`/`REJECTED` with dead-message-queue routing), message replay, shared subscriptions, programmatic topic-to-queue mapping, and native request-reply.

Use the SMF module for new development that needs Solace-native capabilities; use this JMS module when you need JMS semantics such as transacted sessions or structured `MapMessage` payloads. Both modules can be used side by side in one application.

### Key Features

- Support for various messaging patterns (Pub/Sub, Request-Reply, Queuing)
- Seamless integration with Solace PubSub+ event brokers
- High-performance event distribution and reliable message delivery
- Support for secure communication with TLS and authentication
- Simplified production and consumption of events
- GraalVM compatible for native image builds

## Quickstart

### Step 1: Import the module

Import the `solace` module into the Ballerina project.

```ballerina
import ballerinax/solace;
```

### Step 2: Instantiate a new connector

#### Initialize a `solace:MessageProducer`

```ballerina
configurable string brokerUrl = ?;
configurable string messageVpn = ?;
configurable string queueName = ?;
configurable string username = ?;
configurable string password = ?;

solace:MessageProducer producer = check new (brokerUrl,
    destination = {
        queueName
    },
    messageVpn = messageVpn,
    auth = {
        username,
        password
    }
);
```

#### Initialize a `solace:MessageConsumer`

```ballerina
configurable string brokerUrl = ?;
configurable string messageVpn = ?;
configurable string queueName = ?;
configurable string username = ?;
configurable string password = ?;

solace:MessageConsumer consumer = check new (brokerUrl,
    destination = {
        queueName
    },
    messageVpn = messageVpn,
    auth = {
        username,
        password
    }
);
```

### Step 3: Invoke the connector operation

Now, you can use the available connector operations to interact with Solace broker.

#### Produce message to a queue

```ballerina
check producer->send({
    payload: "This is a sample message"
});
```

#### Retrieve a message from a queue

```ballerina
solace:Message? receivedMessage = check consumer->receive(5.0);
```

### Step 4: Run the Ballerina application

Save the changes and run the Ballerina application using the following command.

```bash
bal run
```

## Using the SMF-native API

Import the `solace.smf` module to use the SMF-native API alongside (or instead of) the JMS API:

```ballerina
import ballerinax/solace.smf;

public function main() returns error? {
    smf:PersistentPublisher publisher = check new ("smf://localhost:55555",
        auth = {username: "admin", password: "admin"}
    );
    // Blocks until the broker acknowledges the message (publish receipt)
    check publisher->publish("Hello, World!", "orders/retail/usa");
    check publisher->close();

    smf:PersistentReceiver receiver = check new ("smf://localhost:55555",
        auth = {username: "admin", password: "admin"},
        queueName = "orders"
    );
    smf:Message? message = check receiver->receive(10);
    if message is smf:Message {
        // Settle with ACCEPTED; `failed()` and `rejected()` are available with
        // `negativeSettlementEnabled: true`
        check receiver->ack(message);
    }
    check receiver->close();
}
```

See the `solace.smf` module documentation for direct (at-most-once) publishers and receivers,
shared subscriptions, message replay, services, and request-reply.

## Examples

The Solace connector provides practical examples illustrating usage in various scenarios. Explore
these [examples](https://github.com/ballerina-platform/module-ballerinax-solace/tree/main/examples/).

1. [Order processing (SMF-native)](https://github.com/ballerina-platform/module-ballerinax-solace/tree/main/examples/order-processing/) - Demonstrates guaranteed publish with publish receipts, data binding, explicit acknowledgement, negative settlement (`REJECTED`) of a poison message, and native request-reply using the `solace.smf` module.
