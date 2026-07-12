# Specification: Ballerina `solace.jms` Library

_Authors_: @gayaldassanayake \
_Reviewers_: TBA \
_Created_: 2025/11/21 \
_Updated_: 2025/11/23 \
_Edition_: Swan Lake

## Introduction

This is the specification for the `solace.jms` library of [Ballerina language](https://ballerina.io/), which provides the
functionality to send and receive messages by connecting to a Solace Event Broker via JMS protocol.

The `solace.jms` library specification has evolved and may continue to evolve in the future. The released versions of the
specification can be found under the relevant GitHub tag.

If you have any feedback or suggestions, you can submit a proposal as a pull request to the [ballerina-spec](https://github.com/ballerina-platform/ballerina-spec) repository under the `/beps/con-solace` directory. You can also initiate the related discussion by opening an [issue](https://github.com/ballerina-platform/ballerina-spec/issues) in the same repository. Once the proposal is reviewed and accepted, the corresponding pull request will be merged into the `ballerina-spec` repository.

The conforming implementation of the specification is released to Ballerina Central. Any deviation from the specification is considered a bug.

## Contents

1. [Overview](#1-overview)
2. [Common configuration](#2-common-configuration)
3. [Message](#3-message)
4. [Message producer](#4-message-producer)
    * 4.1. [Configurations](#41-configurations)
    * 4.2. [Initialization](#42-initialization)
    * 4.3. [Functions](#43-functions)
5. [Message consumer](#5-message-consumer)
    * 5.1. [Configurations](#51-configurations)
    * 5.2. [Initialization](#52-initialization)
    * 5.3. [Functions](#53-functions)
6. [Message listener](#6-message-listener)
    * 6.1. [Configurations](#61-configurations)
    * 6.2. [Initialization](#62-initialization)
    * 6.3. [Functions](#63-functions)
    * 6.4. [Service](#64-service)    
     * 6.4.1. [Configuration](#641-configuration)
     * 6.4.2. [Functions](#642-functions)
    * 6.5. [Caller](#65-caller)
     * 6.5.1. [Functions](#651-functions)
    * 6.6. [Usage](#66-usage)

## 1. Overview

Solace Event Broker is a high-performance event-streaming and messaging platform that enables real-time, scalable, and event-driven communication between distributed applications. This specification describes how to use JMS API based clients to connect to Solace event broker. These clients allow the writing of distributed applications and microservices that read, write, and process messages in parallel, at scale, and in a fault-tolerant manner even in the case of network problems or machine failures.

Ballerina `solace.jms` provides several core APIs:

- **`jms:MessageProducer`**: A client endpoint for sending messages to a Solace queue or topic.
- **`jms:MessageConsumer`**: A client endpoint for receiving messages from a Solace queue or topic.
- **`jms:Listener`**: An endpoint that allows a Ballerina service to receive messages from a Solace queue or topic.
- **`jms:Caller`**: A client used within a service to acknowledge messages or manage transactions.

## 2. Common configuration

- `CommonConnectionConfiguration` record represents the common configurations needed for connecting with the Solace event broker.
```ballerina
type CommonConnectionConfiguration record {
    # The name of the message VPN to connect to
    string messageVpn = "default";
    # The authentication configuration. Supports basic authentication, Kerberos, and OAuth2.
    # For client certificate authentication, configure the `secureSocket.keyStore` field
    AuthConfiguration auth?;
    # The SSL/TLS configuration for secure connections
    SecureSocket secureSocket?;
    # A unique client name to use to register to the appliance. If not specified, a unique client ID is auto-generated
    string clientName?;
    # A description for the application client
    string clientDescription = "Ballerina Solace JMS Connector";
    # Enables automatic creation of durable queues and topic endpoints on the broker
    boolean enableDynamicDurables = false;
    # Enables direct transport mode for message delivery. When `true`, uses direct (at-most-once) delivery.
    # When `false`, uses guaranteed (persistent) delivery mode. Direct transport must be disabled for
    # transacted sessions and XA transactions.
    boolean directTransport = true;
    # Enables direct message optimization. When `true`, optimizes message delivery in direct transport mode
    # by reducing protocol overhead. Only applicable when `directTransport` is `true`.
    boolean directOptimized = true;
    # The local interface IP address to bind for outbound connections
    string localhost?;
    # The the maximum amount of time (in seconds) permitted for a JNDI connection attempt.
    # A value of 0 means wait indefinitely
    decimal connectTimeout = 30.0;
    # the maximum amount of time (in seconds) permitted for reading a JNDI lookup reply from the host
    decimal readTimeout = 10.0;
    # The configuration to enable and specify the ZLIB compression level.
    # Valid range is 0-9, where 0 means no compression. Higher values provide better compression at the slower throughput
    int compressionLevel = 0;
    # The retry configuration for connection and reconnection attempts
    RetryConfiguration retryConfig?;
};
```

- `AuthConfiguration` represents the authentication configuration for connecting to a Solace broker.
It is a union of `BasicAuthConfiguration`, `KerberosConfiguration`, and `OAuth2Configuration`.
```ballerina
public type AuthConfiguration BasicAuthConfiguration|KerberosConfiguration|OAuth2Configuration;
```

- `BasicAuthConfiguration` record represents the basic authentication credentials for connecting to a Solace broker.
```ballerina
public type BasicAuthConfiguration record {|
    # The username for authentication
    string username;
    # The password for authentication
    string password?;
|};
```

- `KerberosConfiguration` record represents the Kerberos (GSS-KRB) authentication configuration for connecting to a Solace broker. 
```ballerina
public type KerberosConfiguration record {|
    # The Kerberos service name used during authentication
    string serviceName = "solace";
    # The JAAS login context name to use for authentication
    string jaasLoginContext = "SolaceGSS";
    # Specifies whether to enable Kerberos mutual authentication
    boolean mutualAuthentication = false;
    # Specifies whether to enable automatic reload of the JAAS configuration file
    boolean jaasConfigFileReloadEnabled = false;
|};
```

- `OAuth2Configuration` represents the OAuth 2.0 authentication configuration for connecting to a Solace
broker. It is a union of `OAuth2AccessTokenAuth` and `OidcIdTokenAuth` - exactly one of them must
be provided, and which one is used determines whether an OAuth 2.0 access token or an OIDC ID
token is presented to the broker.
```ballerina
public type OAuth2AccessTokenAuth record {|
    # The OAuth 2.0 issuer identifier URI
    string issuer;
    # The OAuth 2.0 access token for authentication
    string accessToken;
|};

public type OidcIdTokenAuth record {|
    # The OAuth 2.0 issuer identifier URI
    string issuer;
    # The OpenID Connect (OIDC) ID token for authentication
    string oidcToken;
|};

public type OAuth2Configuration OAuth2AccessTokenAuth|OidcIdTokenAuth;
```

- `SecureSocket` record represents the SSL/TLS configuration for secure connections to a Solace broker.
```ballerina
public type SecureSocket record {|
    # The trust store configuration containing trusted CA certificates
    TrustStore trustStore?;
    # The key store configuration containing the client's private key and certificate.
    # When configured, enables client certificate authentication
    KeyStore keyStore?;
    # The SSL/TLS protocols NOT to use
    Protocol[] excludedProtocols = [SSLv2Hello];
    # The list of cipher suites to enable for the connection.
    # If not specified, the default cipher suites for the JVM are used
    SslCipherSuite[] cipherSuites?;
    # The list of acceptable common names for broker certificate validation.
    # If specified, the broker certificate's common name must match one of these values
    string[] trustedCommonNames?;
    # The certificate validation settings
    CertificateValidation validation = {};
|};
```

- `CertificateValidation` record represents the certificate validation settings for SSL/TLS connections to a Solace broker.
```ballerina
public type CertificateValidation record {|
    # Enable certificate validation
    boolean enabled = true;
    # Specifies whether to validate the certificate's expiration date
    boolean validateDate = true;
    # Specifies whether to validate that the certificate's common name matches the broker hostname
    boolean validateHostname = true;
|};
```

- `TrustStore` record represents a trust store containing trusted CA certificates.
```ballerina
public type TrustStore record {|
    # The URL or path of the truststore file
    string location;
    # The password for the trust store
    string password;
    # The format of the trust store file
    SslStoreFormat format = JKS;
|};
```

- `KeyStore` record represents a key store containing the client's private key and certificate. 
```ballerina
public type KeyStore record {|
    # The URL or path of the keystore file
    string location;
    # The password for the key store
    string password;
    # The password for the private key within the key store.
    # If not specified, the key store password is used
    string keyPassword?;
    # The alias of the private key to use from the key store.
    # If not specified, the first private key found is used
    string keyAlias?;
    # The format of the key store file
    SslStoreFormat format = JKS;
|};
```

- `Protocol` type represents the supported SSL/TLS protocol versions.
```ballerina
public type Protocol SSLv30|TLSv10|TLSv11|TLSv12;
```

- `SslCipherSuite` type represents the SSL Cipher Suite to be used for secure communication with the Solace broker.
```ballerina
public type SslCipherSuite ECDHE_RSA_AES256_CBC_SHA384|ECDHE_RSA_AES256_CBC_SHA|RSA_AES256_CBC_SHA256|RSA_AES256_CBC_SHA|
    ECDHE_RSA_3DES_EDE_CBC_SHA|RSA_3DES_EDE_CBC_SHA|ECDHE_RSA_AES128_CBC_SHA|ECDHE_RSA_AES128_CBC_SHA256|RSA_AES128_CBC_SHA256|
    RSA_AES128_CBC_SHA;
```

- `RetryConfiguration` record represents the retry configuration for connection and reconnection attempts to a Solace broker. 
```ballerina
public type RetryConfiguration record {|
    # The number of times to retry connecting to the broker during initial connection.
    # A value of -1 means retry forever, 0 means no retries (fail immediately on first failure)
    int connectRetries = 0;
    # The number of connection retries per host when multiple hosts are specified in the URL.
    # This applies to each host in a comma-separated host list
    int connectRetriesPerHost = 0;
    # The number of times to retry reconnecting after an established connection is lost.
    # A value of -1 means retry forever
    int reconnectRetries = 3;
    # The time to wait between reconnection attempts, in seconds
    decimal reconnectRetryWait = 3.0;
|};
```

- `CommonConsumerConfiguration` record represents the common configurations related to the Solace queue or topic subscription.
```ballerina
public type CommonConsumerConfiguration record {|
    # Configuration indicating how messages received by the session will be acknowledged
    AcknowledgementMode ackMode = AUTO_ACKNOWLEDGE;
    # Only messages with properties matching the message selector expression are delivered. 
    # If this value is not set that indicates that there is no message selector for the message consumer
    # For example, to only receive messages with a property `priority` set to `'high'`, use:
    # `"priority = 'high'"`. If this value is not set, all messages in the queue will be delivered.
    string messageSelector?;
|};
```

- `AcknowledgementMode` enum defines the JMS session acknowledgement modes.
```ballerina
public enum AcknowledgementMode {
    # Indicates that the session will use a local transaction which may subsequently 
    # be committed or rolled back by calling the session's `commit` or `rollback` methods. 
    SESSION_TRANSACTED = "SESSION_TRANSACTED",
    # Indicates that the session automatically acknowledges a client's receipt of a message 
    # either when the session has successfully returned from a call to `receive` or when 
    # the message listener the session has called to process the message successfully returns.
    AUTO_ACKNOWLEDGE = "AUTO_ACKNOWLEDGE",
    # Indicates that the client acknowledges a consumed message by calling the 
    # MessageConsumer's or Caller's `ack` method. Acknowledging a consumed message 
    # acknowledges all messages that the session has consumed.
    CLIENT_ACKNOWLEDGE = "CLIENT_ACKNOWLEDGE",
    # Indicates that the session to lazily acknowledge the delivery of messages. 
    # This is likely to result in the delivery of some duplicate messages if the JMS provider fails, 
    # so it should only be used by consumers that can tolerate duplicate messages. 
    # Use of this mode can reduce session overhead by minimizing the work the session does to prevent duplicates.
    DUPS_OK_ACKNOWLEDGE = "DUPS_OK_ACKNOWLEDGE"
}
```

- `Durability` enum defines the durability of a queue or topic subscription. 
```ballerina
public enum Durability {
    # Represents JMS durable subscriber
    DURABLE,
    # Represents JMS default (non-durable) consumer
    TEMPORARY
}
```

## 3. Message

An Solace message is a fundamental unit of data that facilitates communication between applications and the Solace event broker. It encompasses not only the actual data payload but also includes metadata in the form of headers and customizable properties. This comprehensive structure enables reliable, secure, and flexible data transfer in distributed and enterprise environments.

- `Message` record represent the message used to send and receive content from the Solace broker.
```ballerina
public type Message record {|
    # Message payload
    anydata payload;
    # Id which can be used to correlate multiple messages
    string correlationId?;
    # JMS destination to which a reply to this message should be sent
    Destination replyTo?;
    # Additional message properties
    map<Property> properties?;
    # Unique identifier for a JMS message (Only set by the JMS provider)
    string messageId?;
    # Time a message was handed off to a provider to be sent (Only set by the JMS provider)
    int timestamp?;
    # JMS destination of this message (Only set by the JMS provider)
    Destination destination?;
    # Delivery mode of this message (Only set by the JMS provider)
    int deliveryMode?;
    # Indication of whether this message is being redelivered (Only set by the JMS provider)
    boolean redelivered?;
    # Message type identifier supplied by the client when the message was sent
    string jmsType?;
    # Message expiration time (Only set by the JMS provider)
    int expiration?;
    # Message priority level (Only set by the JMS provider)
    int priority?;
|};
```

- `Destination` type represents a message destination in Solace.
```ballerina
public type Destination Topic|Queue;

# Represents a topic destination for publish/subscribe messaging.
public type Topic record {|
    # The name of the topic. Topics support wildcard subscriptions and multi-level hierarchies 
    # using '/' as a delimiter (e.g., "orders/retail/usa")
    string topicName;
|};

# Represents a queue destination for point-to-point messaging.
public type Queue record {|
    # The name of the queue
    string queueName;
|};
```

- `Property` type represent the valid value types allowed in JMS message properties.
```ballerina
public type Property boolean|int|byte|float|string;
```

## 4. Message producer

The `jms:MessageProducer` is used to send messages to a Solace destination.

### 4.1 Configurations

- `ProducerConfiguration` record represents the configuration for a Solace message producer.
```ballerina
public type ProducerConfiguration record {|
    *jms:CommonConnectionConfiguration;
    # Enables transacted messaging when set to `true`. In transacted mode, messages are sent
    # within a transaction context, requiring explicit commit or rollback
    boolean transacted = false;
    # The default destination (Topic or Queue) where messages will be published. Optional - can be
    # omitted and/or overridden per call via the `destination` parameter of `send()`
    Destination destination?;
|};
```

### 4.2. Initialization

- The `jms:MessageProducer` can be initialized by providing the broker URL and the `jms:ProducerConfiguration`.
```ballerina
# Initializes a new Solace message producer with the given broker URL and configuration.
# ```
# jms:MessageProducer producer = check new (brokerUrl, {
#     destination: {queueName: "orders"},
#     transacted: false
# });
# ```
#
# + url - The Solace broker URL in the format `<scheme>://[username]:[password]@<host>[:port]`.
# Supported schemes are `smf` (plain-text) and `smfs` (TLS/SSL).
# Multiple hosts can be specified as a comma-separated list for failover support.
# Default ports: 55555 (standard), 55003 (compression), 55443 (SSL)
# + config - Producer configuration including connection settings and destination
# + return - A `jms:Error` if initialization fails or else `()`
public isolated function init(string url, *ProducerConfiguration config) returns Error?;
```

### 4.3. Functions

- To send a message to a destination in the Solace event broker, use `send` function. The
`destination` parameter is optional and, when given, takes precedence over the producer's
configured default destination for that call only. If neither a configured default nor a
per-call `destination` is available, `send` returns an `Error`.
```ballerina
# Sends a message to the Solace broker.
# ```
# check producer->send(message);
# check producer->send(message, {queueName: "orders"});
# ```
#
# + message - Message to be sent to the Solace broker
# + destination - The destination (Topic or Queue) to send to for this call, overriding the
# producer's configured default destination, if any
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function send(Message message, Destination? destination = ()) returns Error?;
```

- To commit all messages sent in this transaction and releases any locks currently held, use the `commit` function.
```ballerina
# Commits all messages sent in this transaction and releases any locks currently held.
# This method should only be called when the producer is configured with `transacted: true`.
# ```
# check producer->'commit();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function 'commit() returns Error?;
```

- To roll back any messages sent in this transaction and releases any locks currently held, use the `rollback` function.
```ballerina
# Rolls back any messages sent in this transaction and releases any locks currently held.
# This method should only be called when the producer is configured with `transacted: true`.
# ```
# check producer->'rollback();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function 'rollback() returns Error?;
```

- To close the connection to the message broker and release any underlying resources currently help, use the `close` function.
```ballerina
# Closes the message producer.
# ```
# check producer->close();
# ```
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function close() returns Error?;
```

## 5. Message consumer

The `jms:MessageConsumer` is used to receive messages from a Solace destination.

### 5.1 Configurations

- `ConsumerConfiguration` record represents the configuration for a Solace message consumer.
```ballerina
public type ConsumerConfiguration record {|
    *CommonConnectionConfiguration;
    # The subscription configuration specifying either a queue or topic to consume messages from
    SubscriptionConfiguration subscriptionConfig;
|};
```

- `SubscriptionConfiguration` represents the subscription configuration, either a queue or a topic.
```ballerina
public type SubscriptionConfiguration QueueConfiguration|TopicConfiguration;
```

- `QueueConfiguration` record represents configurations for a Solace queue subscription.
```ballerina
public type QueueConfiguration record {|
    *CommonConsumerConfiguration;
    # The name of the queue to consume messages from - required unless durability is TEMPORARY. Cannot be
    # specified when durability is TEMPORARY (JMS temporary queues are always provider-named)
    string queueName?;
    # DURABLE (pre-provisioned, named queue) or TEMPORARY (auto-deleted when session disconnects)
    Durability durability = DURABLE;
|};
```

- `TopicConfiguration` record represents configurations for Solace topic subscription.
```ballerina
public type TopicConfiguration record {|
    *CommonConsumerConfiguration;
    # The name of the topic to subscribe to
    string topicName;
    # The message consumer durability
    Durability durability = TEMPORARY;
    # The name used to identify the subscription
    string subscriberName?;
    # If true then any messages published to the topic using this session's connection, or any other connection
    # with the same client identifier, will not be added to the durable subscription.
    boolean noLocal = false;
|};
```

### 5.2. Initialization

- The `jms:MessageConsumer` can be initialized by providing the broker URL and the `jms:ConsumerConfiguration`.
```ballerina
# Initializes a new Solace message consumer with the given broker URL and configuration.
# ```
# jms:MessageConsumer consumer = check new (brokerUrl, {
#     subscriptionConfig: {queueName: "orders"}
# });
# ```
#
# + url - The Solace broker URL in the format `<scheme>://[username]:[password]@<host>[:port]`.
# Supported schemes are `smf` (plain-text) and `smfs` (TLS/SSL).
# Multiple hosts can be specified as a comma-separated list for failover support.
# Default ports: 55555 (standard), 55003 (compression), 55443 (SSL)
# + config - Consumer configuration including connection settings and subscription details
# + return - A `jms:Error` if initialization fails or else `()`
public isolated function init(string url, *ConsumerConfiguration config) returns Error?;
```

### 5.3. Functions

- To receives the next message from the Solace broker, use the `receive` function.
```ballerina
# Receives the next message from the Solace broker, waiting up to the specified timeout.
# ```
# jms:Message? message = check consumer->receive(5.0);
# ```
#
# + timeout - The maximum time to wait for a message in seconds. A nil or zero timeout blocks
# indefinitely, matching the underlying JMS default
# + T - Optional type description of the expected data type
# + return - The received `Message`, `()` if no message is available within the timeout, or a `jms:Error` if there is an error
isolated remote function receive(decimal? timeout = (), typedesc<Message> T = <>) returns T|Error?;
```

- To receives the next message from the Solace broker if one is immediately available, use the `receiveNoWait` function.
```ballerina
# Receives the next message from the Solace broker if one is immediately available, without waiting.
# ```
# jms:Message? message = check consumer->receiveNoWait();
# ```
# 
# + T - Optional type description of the expected data type
# + return - The received `Message` if immediately available, `()` if no message is available, or a `jms:Error` if there is an error
isolated remote function receiveNoWait(typedesc<Message> T = <>) returns T|Error?;
```

- To acknowledges the specified message, use the `ack` function.
```ballerina
# Acknowledges the specified message. This method should only be called when the consumer is configured
# with `ackMode: CLIENT_ACKNOWLEDGE`.
# ```
# check consumer->ack(message);
# ```
#
# + message - The message to acknowledge
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function ack(Message message) returns Error?;
```

- To commit all messages received in this transaction and releases any locks currently held, use the `commit` function.
```ballerina
# Commits all messages received in this transaction and releases any locks currently held.
# This method should only be called when the consumer is configured with `ackMode: SESSION_TRANSACTED`.
# ```
# check consumer->'commit();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function 'commit() returns Error?
```

- To roll back any messages received in this transaction and releases any locks currently held, use the `rollback` function.
```ballerina
# Rolls back any messages received in this transaction and releases any locks currently held.
# This method should only be called when the consumer is configured with `ackMode: SESSION_TRANSACTED`.
# ```
# check consumer->'rollback();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function 'rollback() returns Error?;
```

- To close the message consumer and release any underlying resource, use the `close` function.
```ballerina
# Closes the message consumer and releases all resources.
# ```
# check consumer->close();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function close() returns Error?;
```

## 6. Message listener

The `jms:Listener` enables applications to receive messages asynchronously from a Solace event broker.

### 6.1 Configurations

- `ListenerConfiguration` record represents the listener configuration for Ballerina Solace listener.
```ballerina
public type ListenerConfiguration record {|
    *CommonConnectionConfiguration;
|};
```

### 6.2. Initialization

- The `jms:Listener` can be initialized by providing the broker URL and the `jms:ListenerConfiguration`.
```ballerina
# Initializes a new Solace message listener with the given broker URL and configuration.
# ```
# listener jms:Listener messageListener = check new (
#     url = "smf://localhost:55554",
#     messageVpn = "default",
#     auth = {
#         username: "admin",
#         password: "admin"
#     }
# );
# ```
#
# + url - The Solace broker URL in the format `<scheme>://[username]:[password]@<host>[:port]`.
# Supported schemes are `smf` (plain-text) and `smfs` (TLS/SSL).
# Multiple hosts can be specified as a comma-separated list for failover support.
# Default ports: 55555 (standard), 55003 (compression), 55443 (SSL)
# + config - configurations used when initializing the listener
# + return - `jms:Error` if an error occurs or `()` otherwise
public isolated function init(string url, *ListenerConfiguration config) returns Error?;
```

### 6.3. Functions

- To attach a service to the listener, use the `attach` function.
```ballerina
# Attaches a Solace service to the listener.
# ```
# check messageListener.attach(solaceSvc);
# ```
#
# + 'service - service instance
# + name - service name
# + return - `jms:Error` if an error occurs or `()` otherwise
public isolated function attach(Service 'service, string[]|string? name = ()) returns Error?;
```

- To detach a service from the listener, use the `detach` function.
```ballerina
# Detaches a Solace service from the listener.
# ```
# check messageListener.detach(solaceSvc);
# ```
#
# + 'service - service instance
# + return - `jms:Error` if an error occurs or `()` otherwise
public isolated function detach(Service 'service) returns Error?;
```

- To start the listener, use the `'start` function.
```ballerina
# Starts the listener.
# ```
# check messageListener.'start();
# ```
#
# + return - `jms:Error` if an error occurs or `()` otherwise
public isolated function 'start() returns Error?;
```

- To stop the listener gracefully, use the `gracefulStop` function.
```ballerina
# Gracefully stops the listener.
# ```
# check messageListener.gracefulStop();
# ```
#
# + return - `jms:Error` if an error occurs or `()` otherwise
public isolated function gracefulStop() returns Error?;
```

- To stop the listener immediately, use the `immediateStop` function.
```ballerina
# Immediately stops the listener.
# ```
# check messageListener.immediateStop();
# ```
#
# + return - `jms:Error` if an error occurs or `()` otherwise
public isolated function immediateStop() returns Error?;
```

### 6.4. Service

A Solace service in Ballerina is used to receive messages from a Solace. It is attached to a `jms:Listener` and bound to a specific Solace destination, which can be either a **queue** or a **topic**.

#### 6.4.1. Configuration

- `ServiceConfig` defines the configurations for the service.
```ballerina
public annotation ServiceConfiguration ServiceConfig on service;
```

- `ServiceConfiguration` type defines the service configuration types for a Solace service.
```ballerina
public type ServiceConfiguration QueueServiceConfiguration|TopicServiceConfiguration;
```

- `QueueServiceConfiguration` record represents configurations for a service configurations related to solace queue subscription.
```ballerina
public type QueueServiceConfiguration record {|
    *CommonServiceConfiguration;
    # The name of the queue to consume messages from
    string queueName;
|};
```

- `TopicServiceConfiguration` record represents configurations for a service configurations related to solace topic subscription.
```ballerina
public type TopicServiceConfiguration record {|
    *CommonServiceConfiguration;
    # The name of the topic to subscribe to
    string topicName;
    # The message consumer durability
    Durability durability = TEMPORARY;
    # The name used to identify the subscription
    string subscriberName?;
    # If true then any messages published to the topic using this session's connection, or any other connection
    # with the same client identifier, will not be added to the durable subscription.
    boolean noLocal = false;
|};
```

#### 6.4.2. Functions

- To receive messages from a Solace destination, use the `onMessage` function.
```ballerina
# Invoked when a message is received at a subscribed Solace destination.
#
# + message - Received Solace message
# + caller - Optional `jms:Caller` to control transactions and message acknowledgement
# + return - A `error` if there is an error during message processing or else `()`
remote function onMessage(jms:Message message, jms:Caller caller) returns error?;
```

- To handle runtime errors that occur while dispatching a message to the `onMessage` function, use the `onError` function. `onError` is an optional API, if the user does not define a `onError` function on the `jms:Service` the identified error will be logged into the console.
```ballerina
# Invoked when a runtime error occurs during message while dispatching a message to the `onMessage` method.
#
# + err - The `jms:Error` containing details of the error encountered
# + return - A `error` if an error occurs while handling the error, or else `()`
remote function onError(jms:Error err) returns error?;
```

### 6.5. Caller

The `jms:Caller` is used inside a `jms:Service` to acknowledge a message or to handle transactions.

#### 6.5.1. Functions

- To mark a Solace message as received, use the `ack` function.
```ballerina
# Mark a Solace message as received.
# ```
# check caller->ack(message);
# ```
#
# + message - Solace message record
# + return - `jms:Error` if there is an error in the execution or else '()'
isolated remote function ack(jms:Message message) returns Error?;
```

- To commit all the messages received in this transaction and release any locks currently held, use the `commit` function.
```ballerina
# Commits all messages received in this transaction and releases any locks currently held.
# ```
# check caller->'commit();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function 'commit() returns Error?;
```

- To rollback all the messages received in this transaction and release any locks currently held, use the `rollback` function.
```ballerina
# Rolls back any messages received in this transaction and releases any locks currently held.
# ```
# check caller->'rollback();
# ```
#
# + return - A `jms:Error` if there is an error or else `()`
isolated remote function 'rollback() returns Error?;
```

### 6.6. Usage

After initializing the `jms:Listener` a `jms:Service` must be attached to it.
```ballerina
@jms:ServiceConfig {
   queueName: "MyQueue"
}
service on messageListener {
    remote function onMessage(jms:Message message, jms:Caller caller) returns error? {
        // process results
    }
}
```
