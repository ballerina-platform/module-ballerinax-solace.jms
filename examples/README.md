# Examples

The `solace.jms` package provides practical examples illustrating its usage in various real-world scenarios. Explore these [examples](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples) to understand how to produce, consume, and reliably process messages with a Solace event broker.

1. [Order Fulfillment](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples/order-fulfillment) - Send orders to a queue and process them with `CLIENT_ACKNOWLEDGE` mode. Shows point-to-point (queue) messaging where a message is only acknowledged once it has been fulfilled successfully, so a worker that crashes beforehand picks it back up on restart.

2. [Live Price Alerts](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples/live-price-alerts) - Publish stock price updates to hierarchical topics and raise alerts only for significant moves. Shows publish/subscribe (topic) messaging with topic wildcards and a JMS message selector.

3. [Transactional Inventory Sync](https://github.com/ballerina-platform/module-ballerinax-solace.jms/tree/main/examples/transactional-inventory-sync) - Apply inventory deltas from a queue within a `SESSION_TRANSACTED` session, rolling back and safely discarding a bad update instead of corrupting inventory state. Shows transacted sessions with `commit`/`rollback`.

## Prerequisites

All examples connect to a local Solace PubSub+ broker. Start one with Docker before running any example:

```bash
docker compose -f examples/docker-compose.yaml up -d
```

This starts a broker reachable at `smf://localhost:55554`, message VPN `default`, with credentials `admin`/`admin` - the defaults every example already uses.

## Running an Example

Each example is made up of one or more independent Ballerina projects (for example, a producer and a consumer). Build and run each one from its own directory:

```bash
bal run
```

Where an example has more than one project, run them in the order given in that example's own instructions - it varies depending on whether the messaging is queue-based (durable, so order usually doesn't matter) or topic-based (live, so the subscriber must be running first).

See the root [README](../README.md#build-from-the-source) for the full local build setup.
