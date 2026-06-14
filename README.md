# Ballerina Solace connector

[![Build](https://github.com/ballerina-platform/module-ballerinax-solace/actions/workflows/ci.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-solace/actions/workflows/ci.yml)
[![Trivy](https://github.com/ballerina-platform/module-ballerinax-solace/actions/workflows/trivy-scan.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-solace/actions/workflows/trivy-scan.yml)
[![GraalVM Check](https://github.com/ballerina-platform/module-ballerinax-solace/actions/workflows/build-with-bal-test-graalvm.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerinax-solace/actions/workflows/build-with-bal-test-graalvm.yml)
[![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerinax-solace.svg)](https://github.com/ballerina-platform/module-ballerinax-solace/commits/main)
[![GitHub Issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-library/module/solace.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-library/labels/module%2Fsolace)

[Solace PubSub+](https://docs.solace.com/) is an advanced event-broker platform that enables event-driven communication across distributed applications using multiple messaging patterns such as publish/subscribe, request/reply, and queue-based messaging. It supports standard messaging protocols, including JMS, MQTT, AMQP, and REST, enabling seamless integration across diverse systems and environments.

The `ballerinax/solace` package provides two complementary API surfaces for interacting with Solace PubSub+ brokers:

- **`ballerinax/solace`** (the default module) - a JMS-based API built on the Solace JMS driver, providing `MessageProducer`, `MessageConsumer`, and a `Listener` with standard JMS semantics.
- **`ballerinax/solace.smf`** - an SMF-native API built on the Solace PubSub+ Messaging API for Java, exposing capabilities the JMS abstraction hides: per-publisher/per-receiver quality of service, publish receipts with back-pressure control, negative message settlement (NACK), message replay, shared subscriptions, and native request-reply.

Existing users of the JMS-based API are unaffected by the SMF module: the JMS surface keeps its names and semantics, and both modules ship in the same package and can be used side by side in one application.

## Choosing between the two API surfaces

| Capability | JMS (`ballerinax/solace`) | SMF (`ballerinax/solace.smf`) |
|---|---|---|
| Queue point-to-point and topic pub/sub | ✅ | ✅ |
| Transacted sessions (local transactions) | ✅ only | ❌ (not supported by the underlying API) |
| `CLIENT_ACKNOWLEDGE` (cumulative) acknowledgement | ✅ | per-message settlement instead |
| Per-message settlement `ACCEPTED`/`FAILED`/`REJECTED` (NACK, DMQ routing) | ❌ | ✅ only |
| Message replay (all messages / time-based / replication-group id) | ❌ | ✅ only |
| Shared subscriptions (load-balanced direct consumers) | ❌ | ✅ only |
| Durable topic subscriptions | ✅ (durable subscriber) | via queue + topic-to-queue mapping |
| Programmatic topic-to-queue mapping | ❌ | ✅ only |
| Endpoint provisioning | `enableDynamicDurables` (connection-level) | `missingResourcesStrategy` (per-receiver) |
| Native request-reply | manual `replyTo` only | ✅ first-class (`Requester`/`Replier`) |
| Message selectors | ✅ | ✅ (persistent receiver) |
| Per-publisher/per-receiver QoS | ❌ (connection-level `directTransport`) | ✅ |
| Publish receipts + publisher back-pressure configuration | ❌ | ✅ |
| Structured map payloads (SDT `MapMessage`) | ✅ only | ❌ (not exposed by the underlying API) |
| Typed message properties | ✅ | string-typed properties only |
| Basic / Kerberos / OAuth2 / client-certificate auth, TLS, compression, multi-host failover | ✅ | ✅ |

**Rule of thumb:** use the SMF module for new development that needs Solace-native capabilities; use the JMS module when you need JMS semantics such as transacted sessions or `MapMessage` payloads.

## Examples

The connector comes with runnable examples under the [`examples`](./examples/) directory.

1. [Order processing (SMF-native)](./examples/order-processing/) - Demonstrates guaranteed publish with publish receipts, data binding, explicit acknowledgement, negative settlement (`REJECTED`) of a poison message, and native request-reply using the `solace.smf` module.

## Build from the source

### Setting up the prerequisites

1. Download and install Java SE Development Kit (JDK) version 21. You can download it from either of the following sources:

    * [Oracle JDK](https://www.oracle.com/java/technologies/downloads/)
    * [OpenJDK](https://adoptium.net/)

   > **Note:** After installation, remember to set the `JAVA_HOME` environment variable to the directory where JDK was installed.

2. Download and install [Ballerina Swan Lake](https://ballerina.io/).

3. Download and install [Docker](https://www.docker.com/get-started).

   > **Note**: Ensure that the Docker daemon is running before executing any tests.

4. Export Github Personal access token with read package permissions as follows,

    ```bash
    export packageUser=<Username>
    export packagePAT=<Personal access token>
    ```

### Build options

Execute the commands below to build from the source.

1. To build the package:

   ```bash
   ./gradlew clean build
   ```

2. To run the tests:

   ```bash
   ./gradlew clean test
   ```

3. To build the without the tests:

   ```bash
   ./gradlew clean build -x test
   ```

4. To run tests against different environments:

   ```bash
   ./gradlew clean test -Pgroups=<Comma separated groups/test cases>
   ```

5. To debug the package with a remote debugger:

   ```bash
   ./gradlew clean build -Pdebug=<port>
   ```

6. To debug with the Ballerina language:

   ```bash
   ./gradlew clean build -PbalJavaDebug=<port>
   ```

7. Publish the generated artifacts to the local Ballerina Central repository:

    ```bash
    ./gradlew clean build -PpublishToLocalCentral=true
    ```

8. Publish the generated artifacts to the Ballerina Central repository:

   ```bash
   ./gradlew clean build -PpublishToCentral=true
   ```

## Contribute to Ballerina

As an open-source project, Ballerina welcomes contributions from the community.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of conduct

All the contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful links

* For more information go to the [`solace` package](https://central.ballerina.io/ballerinax/solace/latest).
* For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/).
* Chat live with us via our [Discord server](https://discord.gg/ballerinalang).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
