## Overview

Solace PubSub+ is a powerful event broker that supports multiple protocols and messaging patterns. It provides high-performance, reliable, and scalable messaging for modern event-driven architectures. The Solace connector allows you to integrate with Solace event brokers, enabling efficient event distribution across various environments.

The `solace.jms` connector uses the [Java Message Service (JMS) API](https://jakarta.ee/specifications/messaging/) to produce, consume, and listen for messages on Solace PubSub+ queues and topics.

### Key Features

- Support for various messaging patterns (Pub/Sub, Request-Reply, Queuing)
- Seamless integration with Solace PubSub+ event brokers
- High-performance event distribution and reliable message delivery
- Support for secure communication with TLS and authentication
- Simplified production and consumption of events
- GraalVM compatible for native image builds

## Setup guide

To try out the `solace.jms` connector, you need a running Solace PubSub+ broker. The quickest way to get one locally is with Docker.

### Step 1: Start a Solace PubSub+ broker with Docker

Run the following command to start a Solace PubSub+ standard broker container:

```bash
docker run -d --name solace \
    -p 55554:55555 -p 55003:55003 -p 8080:8080 \
    --shm-size=1g \
    -e username_admin_globalaccesslevel=admin \
    -e username_admin_password=admin \
    solace/solace-pubsub-standard:latest
```

Once the container is up and running, the broker is reachable at `smf://localhost:55554` on the default message VPN `default`, with the credentials `admin`/`admin`. These are the same defaults used throughout the samples in this guide.

### Step 2: Create a queue

The samples below produce and consume messages on a queue, so create one before running them:

1. Open the Solace PubSub+ Manager at [http://localhost:8080](http://localhost:8080) and log in with `admin`/`admin`.
2. Navigate to the `default` message VPN > **Queues** and click **+ Queue**.
3. Give the queue a name (for example, `sample-queue`) and save it.

## Quickstart

### Step 1: Import the module

Import the `solace.jms` module into the Ballerina project.

```ballerina
import ballerinax/solace.jms;
```

### Step 2: Instantiate a new connector

#### Initialize a `jms:MessageProducer`

```ballerina
configurable string brokerUrl = ?;
configurable string messageVpn = ?;
configurable string queueName = ?;
configurable string username = ?;
configurable string password = ?;

jms:MessageProducer producer = check new (brokerUrl,
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

#### Initialize a `jms:MessageConsumer`

```ballerina
configurable string brokerUrl = ?;
configurable string messageVpn = ?;
configurable string queueName = ?;
configurable string username = ?;
configurable string password = ?;

jms:MessageConsumer consumer = check new (brokerUrl,
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

#### Initialize a `jms:Listener`

```ballerina
configurable string brokerUrl = ?;
configurable string messageVpn = ?;
configurable string queueName = ?;
configurable string username = ?;
configurable string password = ?;

listener jms:Listener solaceListener = check new (brokerUrl,
    messageVpn = messageVpn,
    auth = {
        username,
        password
    }
);

@jms:ServiceConfig {
    queueName
}
service on solaceListener {
    remote function onMessage(jms:Message message) returns error? {
        // Process the received message
    }
}
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
jms:Message? receivedMessage = check consumer->receive(5.0);
```

A `jms:Listener` does not need to be explicitly invoked - once attached, the `onMessage` remote method is called automatically for every message on the queue as soon as the listener starts.

### Step 4: Run the Ballerina application

Save the changes and run the Ballerina application using the following command.

```bash
bal run
```

## Examples

The `solace.jms` connector provides practical examples illustrating usage in various scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples), covering the following use cases:

1. [Order Fulfillment](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples/order-fulfillment) - Send orders to a queue and process them with `CLIENT_ACKNOWLEDGE` mode. Shows point-to-point (queue) messaging where a message is only acknowledged once it has been fulfilled successfully, so a worker that crashes beforehand picks it back up on restart.

2. [Live Price Alerts](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples/live-price-alerts) - Publish stock price updates to hierarchical topics and raise alerts only for significant moves. Shows publish/subscribe (topic) messaging with topic wildcards and a JMS message selector.

3. [Transactional Inventory Sync](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples/transactional-inventory-sync) - Apply inventory deltas from a queue within a `SESSION_TRANSACTED` session, rolling back and safely discarding a bad update instead of corrupting inventory state. Shows transacted sessions with `commit`/`rollback`.
