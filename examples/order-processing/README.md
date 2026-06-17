# Order processing — Solace SMF example

An end-to-end example of the SMF-native API (`ballerinax/solace.smf`). It demonstrates the
capabilities the JMS surface cannot expose:

1. **Guaranteed publish + receive** — a `PersistentPublisher` publishes orders and blocks until the
   broker confirms each one (publish receipt). A `PersistentReceiver` provisions its own queue
   (`CREATE_ON_START`), binds the order topic to it, data-binds each message to an `Order` record,
   and acknowledges it explicitly.
2. **Negative settlement** — a poison message is settled with the `REJECTED` outcome and is verified
   not to be redelivered.
3. **Native request-reply** — a `Requester` and `Replier` perform a request-reply round-trip.

## Prerequisites

Run a Solace PubSub+ broker locally:

```bash
docker run -d -p 55554:55555 -p 8080:8080 --shm-size=1g \
  -e username_admin_globalaccesslevel=admin -e username_admin_password=admin \
  solace/solace-pubsub-standard:10.25.24
```

The example provisions its own queues, so no manual broker configuration is required.

## Run

```bash
bal run
```

Connection settings are `configurable` and default to `smf://localhost:55554` with `admin`/`admin`.
Override them with a `Config.toml`:

```toml
brokerUrl = "smf://your-broker:55555"
username = "your-user"
password = "your-password"
```

## Expected output

```text
=== Solace SMF order-processing example ===

[1] Persistent publish + receive with data binding and ack
    published ORD-1 (receipt confirmed by broker)
    published ORD-2 (receipt confirmed by broker)
    received ORD-1: 2 x keyboard (redelivered=false)
    received ORD-2: 1 x monitor (redelivered=false)
    ✓ all orders received and acknowledged

[2] Negative settlement (REJECTED) on a poison message
    received poison order ORD-BAD, rejecting it
    ✓ rejected message was not redelivered

[3] Native request-reply (pricing service)
    quote received: price of keyboard = $49.99
    ✓ request-reply round-trip completed

=== Example completed successfully ===
```
