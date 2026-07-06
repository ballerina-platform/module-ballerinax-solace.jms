# Live Price Alerts

This example demonstrates Solace's other core messaging model: publish/subscribe over topics, using hierarchical topic names, a wildcard subscription, and a JMS message selector together.

A `price_publisher` publishes stock price updates to topics named after the exchange and symbol, for example `stocks/nasdaq/aapl`. An `alert_subscriber` service subscribes to `stocks/nasdaq/*`, so it only receives NASDAQ updates (an IBM update published to `stocks/nyse/ibm` never reaches it), and adds a `messageSelector` of `changePercent > 5.0`, so only significant price moves are dispatched to `onMessage` at all - small moves on subscribed topics are filtered out by the broker itself.

This shows how to narrow both *which topics* a service cares about (via wildcards) and *which messages on those topics* actually matter (via a selector), without writing any filtering logic in the service itself.

## Prerequisites

Start a local Solace broker (see the [examples README](../README.md#prerequisites)):

```bash
docker compose -f ../docker-compose.yaml up -d
```

## Running the Example

Topic subscriptions are live - a message published while no subscriber is connected is not delivered later. Start the subscriber first:

```bash
cd alert_subscriber
bal run
```

In a separate terminal, run the publisher:

```bash
cd price_publisher
bal run
```

The publisher logs all three price updates. The subscriber only logs an alert for GOOGL (+6.8%): AAPL (+2.1%) is below the selector's threshold, and IBM (+9.4%, well above the threshold) is never delivered at all since it's published on `stocks/nyse/ibm`, outside the `stocks/nasdaq/*` subscription.
