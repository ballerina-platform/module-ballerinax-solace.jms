// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

# The Solace JMS service type attached to a `jms:Listener` for asynchronous (push-based) consumption.
#
# An attached service must declare a remote `onMessage` method and may optionally declare an
# `onError` method. The accepted signatures are:
# ```ballerina
# remote function onMessage(record {|*jms:Message; T payload;|} message) returns jms:Error?;
# remote function onMessage(record {|*jms:Message; T payload;|} message, jms:Caller caller) returns jms:Error?;
# remote function onMessage(jms:Message message) returns jms:Error?;
# remote function onError(jms:Error err) returns jms:Error?;
# ```
# Declaring a narrowed `payload` type (`T`) causes the message payload to be data-bound into that
# type; declaring the base `jms:Message` type yields the raw payload as `anydata`.
# The subscription (queue or topic) and flow options are supplied via the
# `@jms:ServiceConfig` annotation on the service.
public type Service distinct service object {
};

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

# Represents a message destination in Solace.
# Can be either a Topic for publish/subscribe messaging or a Queue for point-to-point messaging.
public type Destination Topic|Queue;

# Defines the JMS session acknowledgement modes.
public enum AcknowledgementMode {
    # Indicates that the session will use a local transaction which may subsequently
    # be committed or rolled back by calling the session's `commit` or `rollback` methods.
    SESSION_TRANSACTED,
    # Indicates that the session automatically acknowledges a client's receipt of a message
    # either when the session has successfully returned from a call to `receive` or when
    # the message listener the session has called to process the message successfully returns.
    AUTO_ACKNOWLEDGE,
    # Indicates that the client acknowledges a consumed message by calling the
    # MessageConsumer's or Caller's `ack` method. Acknowledging a consumed message
    # acknowledges all messages that the session has consumed.
    CLIENT_ACKNOWLEDGE,
    # Indicates that the session to lazily acknowledge the delivery of messages.
    # This is likely to result in the delivery of some duplicate messages if the JMS provider fails,
    # so it should only be used by consumers that can tolerate duplicate messages.
    # Use of this mode can reduce session overhead by minimizing the work the session does to prevent duplicates.
    DUPS_OK_ACKNOWLEDGE
}

# Represents the basic authentication credentials for connecting to a Solace broker.
public type BasicAuthConfiguration record {|
    # The username for authentication
    string username;
    # The password for authentication
    string password?;
|};

# Represents the Kerberos (GSS-KRB) authentication configuration for connecting to a Solace broker
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

# Represents the OAuth 2.0 Access Token authentication configuration for connecting to a Solace broker
public type OAuth2AccessTokenAuth record {|
    # The OAuth 2.0 issuer identifier URI
    string issuer;
    # The OAuth 2.0 access token for authentication
    string accessToken;
|};

# Represents the OpenID Connect (OIDC) ID Token authentication configuration for connecting to a Solace broker
public type OidcIdTokenAuth record {|
    # The OAuth 2.0 issuer identifier URI
    string issuer;
    # The OpenID Connect (OIDC) ID token for authentication
    string oidcToken;
|};

# Represents the OAuth 2.0 authentication configuration for connecting to a Solace broker
# (mutually exclusive - use either access token or ID token)
public type OAuth2Configuration OAuth2AccessTokenAuth|OidcIdTokenAuth;

# Represents the authentication configuration for connecting to a Solace broker
# (basic, Kerberos, or OAuth2)
public type AuthConfiguration BasicAuthConfiguration|KerberosConfiguration|OAuth2Configuration;

# Represents the certificate validation settings for SSL/TLS connections to a Solace broker
public type CertificateValidation record {|
    # Enable certificate validation
    boolean enabled = true;
    # Specifies whether to validate the certificate's expiration date
    boolean validateDate = true;
    # Specifies whether to validate that the certificate's common name matches the broker hostname
    boolean validateHostname = true;
|};

# Java KeyStore format
public const JKS = "jks";
# PKCS12 format
public const PKCS12 = "pkcs12";

# Represents the supported SSL store formats.
public type SslStoreFormat JKS|PKCS12;

# Represents a trust store containing trusted CA certificates.
public type TrustStore record {|
    # The URL or path of the truststore file
    string location;
    # The password for the trust store
    string password;
    # The format of the trust store file
    SslStoreFormat format = JKS;
|};

# Represents a key store containing the client's private key and certificate.
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

# Represents the supported SSL/TLS protocol versions.
public enum Protocol {
    TLSV1_1,
    TLSV1_2,
    TLSV1_3
}

# Cipher suite: TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384
public const ECDHE_RSA_AES256_CBC_SHA384 = "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384";
# Cipher suite: TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA
public const ECDHE_RSA_AES256_CBC_SHA = "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA";
# Cipher suite: TLS_RSA_WITH_AES_256_CBC_SHA256
public const RSA_AES256_CBC_SHA256 = "TLS_RSA_WITH_AES_256_CBC_SHA256";
# Cipher suite: TLS_RSA_WITH_AES_256_CBC_SHA
public const RSA_AES256_CBC_SHA = "TLS_RSA_WITH_AES_256_CBC_SHA";
# Cipher suite: TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA
public const ECDHE_RSA_3DES_EDE_CBC_SHA = "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA";
# Cipher suite: SSL_RSA_WITH_3DES_EDE_CBC_SHA
public const RSA_3DES_EDE_CBC_SHA = "SSL_RSA_WITH_3DES_EDE_CBC_SHA";
# Cipher suite: TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA
public const ECDHE_RSA_AES128_CBC_SHA = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA";
# Cipher suite: TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256
public const ECDHE_RSA_AES128_CBC_SHA256 = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256";
# Cipher suite: TLS_RSA_WITH_AES_128_CBC_SHA256
public const RSA_AES128_CBC_SHA256 = "TLS_RSA_WITH_AES_128_CBC_SHA256";
# Cipher suite: TLS_RSA_WITH_AES_128_CBC_SHA
public const RSA_AES128_CBC_SHA = "TLS_RSA_WITH_AES_128_CBC_SHA";

# The SSL Cipher Suite to be used for secure communication with the Solace broker.
public type SslCipherSuite ECDHE_RSA_AES256_CBC_SHA384|ECDHE_RSA_AES256_CBC_SHA|RSA_AES256_CBC_SHA256|RSA_AES256_CBC_SHA|
    ECDHE_RSA_3DES_EDE_CBC_SHA|RSA_3DES_EDE_CBC_SHA|ECDHE_RSA_AES128_CBC_SHA|ECDHE_RSA_AES128_CBC_SHA256|RSA_AES128_CBC_SHA256|
    RSA_AES128_CBC_SHA;

# Represents the SSL/TLS configuration for secure connections to a Solace broker
public type SecureSocket record {|
    # The trust store configuration containing trusted CA certificates
    TrustStore trustStore?;
    # The key store configuration containing the client's private key and certificate.
    # When configured, enables client certificate authentication
    KeyStore keyStore?;
    # The list of acceptable common names for broker certificate validation.
    # If specified, the broker certificate's common name must match one of these values
    string[] trustedCommonNames?;
    # The SSL/TLS protocols NOT to use. None are excluded by default
    Protocol[] excludedProtocols = [];
    # The list of cipher suites to enable for the connection.
    # If not specified, the default cipher suites for the JVM are used
    SslCipherSuite[] cipherSuites?;
    # The certificate validation settings
    CertificateValidation validation = {};
|};

# Represents the retry configuration for connection and reconnection attempts to a Solace broker
public type RetryConfiguration record {|
    # The number of times to retry connecting to the broker during initial connection.
    # A value of -1 means retry forever, 0 means no retries (fail immediately on first failure)
    int connectRetries = 0;
    # The number of connection retries per host when multiple hosts are specified in the URL.
    int connectRetriesPerHost = 0;
    # The number of times to retry reconnecting after an established connection is lost.
    # A value of -1 means retry forever
    int reconnectRetries = 3;
    # The time to wait between reconnection attempts, in seconds
    decimal reconnectRetryWait = 3.0;
|};

# Common connection configuration shared between producer and consumer
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
    # Enables Solace's direct transport optimization for `NON_PERSISTENT` messages. When `true`, `NON_PERSISTENT`
    # messages are sent as genuinely at-most-once direct messages; when `false`, they
    # are still routed through the guaranteed-messaging path but marked non-persistent. Has no effect on
    # `PERSISTENT` messages, which always use guaranteed delivery regardless of this value. Must be `false` for
    # transacted sessions (producer `transacted: true`, or consumer/listener `ackMode: SESSION_TRANSACTED`) -
    # Solace rejects transacted session creation when direct transport is enabled.
    boolean directTransport = true;
    # Enables direct message optimization. When `true`, optimizes message delivery in direct transport mode
    # by reducing protocol overhead. Only applicable when `directTransport` is `true`.
    boolean directOptimized = true;
    # The configuration to enable and specify the ZLIB compression level.
    # Valid range is 0-9, where 0 means no compression. Higher values provide better compression at the slower throughput
    int compressionLevel = 0;
    # The local interface IP address to bind for outbound connections
    string localhost?;
    # The the maximum amount of time (in seconds) permitted for a JNDI connection attempt.
    # A value of 0 means wait indefinitely
    decimal connectTimeout = 30.0;
    # the maximum amount of time (in seconds) permitted for reading a JNDI lookup reply from the host
    decimal readTimeout = 10.0;
    # The retry configuration for connection and reconnection attempts
    RetryConfiguration retryConfig?;
};

type CommonConsumerConnectionConfiguration record {|
    *CommonConnectionConfiguration;
    # Maximum number of un-acknowledged guaranteed (persistent) messages the broker may have in flight
    # to this connection at once. Only applies when `directTransport` is `false`. Valid range: 1-255
    int transportWindowSize?;
    # Percentage of `transportWindowSize` that must be consumed before an acknowledgement is sent back
    # to the broker to slide the window forward. Only applies when `directTransport` is `false`.
    # Valid range: 1-75.
    int ackThreshold = 60;
    # Maximum time (in seconds) the client buffers acknowledgements before flushing them to the broker,
    # independent of `ackThreshold`. Only applies when `directTransport` is `false`. Valid range: 0.02-1.5
    decimal ackTimer?;
|};

# Represents the configuration for a Solace message producer.
public type ProducerConfiguration record {|
    *CommonConnectionConfiguration;
    # Enables transacted messaging when set to `true`. In transacted mode, messages are sent
    # within a transaction context, requiring explicit commit or rollback
    boolean transacted = false;
    # The default destination (Topic or Queue) where messages will be published. Can be omitted
    # and/or overridden per call via the `destination` parameter of `MessageProducer.send()`
    Destination destination?;
|};

# Represents the listener configuration for Ballerina Solace listener.
public type ListenerConfiguration record {|
    *CommonConsumerConnectionConfiguration;
|};

# Common configurations related to the Solace queue or topic subscription.
type CommonConsumerConfiguration record {|
    # Configuration indicating how messages received by the session will be acknowledged
    AcknowledgementMode ackMode = AUTO_ACKNOWLEDGE;
    # Only messages with properties matching the message selector expression are delivered.
    # If this value is not set that indicates that there is no message selector for the message consumer
    # For example, to only receive messages with a property `priority` set to `'high'`, use:
    # `"priority = 'high'"`. If this value is not set, all messages in the queue will be delivered.
    string messageSelector?;
|};

# Represents configurations for a Solace queue subscription.
public type QueueConfiguration record {|
    *CommonConsumerConfiguration;
    # The name of the queue to consume messages from - REQUIRED unless durability is TEMPORARY. Cannot be
    # specified when durability is TEMPORARY (JMS temporary queues are always provider-named)
    string queueName?;
    # DURABLE (pre-provisioned, named queue) or TEMPORARY (auto-deleted when session disconnects)
    Durability durability = DURABLE;
|};

# Represents configurations for Solace topic subscription.
public type TopicConfiguration record {|
    *CommonConsumerConfiguration;
    # The name of the topic to subscribe to
    string topicName;
    # The message consumer durability
    Durability durability = TEMPORARY;
    # The name used to identify the durable subscription
    string subscriberName?;
|};

# Consumer subscription configuration (queue or topic).
public type SubscriptionConfiguration QueueConfiguration|TopicConfiguration;

# Represents the configuration for a Solace message consumer.
public type ConsumerConfiguration record {|
    *CommonConsumerConnectionConfiguration;
    # The subscription configuration specifying either a queue or topic to consume messages from
    SubscriptionConfiguration subscriptionConfig;
|};

# Defines the JMS message delivery modes.
public enum DeliveryMode {
    # The lowest-overhead delivery mode - the JMS provider does not log the message to stable
    # storage, so it may be lost if the provider fails.
    NON_PERSISTENT,
    # Instructs the JMS provider to log the message to stable storage as part of the send
    # operation, so it is not lost unless the provider suffers a hard media failure.
    PERSISTENT
}

# Durability of a queue or topic subscription: DURABLE (persisted/named) or TEMPORARY (ephemeral)
public enum Durability {
    # Represents JMS durable subscriber
    DURABLE,
    # Represents JMS default (non-durable) consumer
    TEMPORARY
}

# Represents configurations for a service configurations related to solace queue subscription.
public type QueueServiceConfiguration record {|
    *CommonConsumerConfiguration;
    # The name of the queue to consume messages from
    string queueName;
|};

# Represents configurations for a service configurations related to solace topic subscription.
public type TopicServiceConfiguration record {|
    *CommonConsumerConfiguration;
    # The name of the topic to subscribe to
    string topicName;
    # The message consumer durability
    Durability durability = TEMPORARY;
    # The name used to identify the durable subscription
    string subscriberName?;
|};

# The service configuration type for the `jms:Service`.
public type ServiceConfiguration QueueServiceConfiguration|TopicServiceConfiguration;

# Represent the Message used to send and receive content from the Solace broker.
public type Message record {|
    # Message payload
    anydata payload;
    # Delivery mode for the message (`NON_PERSISTENT` or `PERSISTENT`)
    DeliveryMode deliveryMode = PERSISTENT;
    # Priority level for the message (0-9, where 9 is the highest)
    int priority?;
    # Time in seconds before this message expires and is discarded (or moved to a Dead Message
    # Queue, if eligible) by the broker. `0` or unset means the message never expires (default)
    decimal timeToLive?;
    # Message type identifier supplied by the client when the message was sent
    string messageType?;
    # Id which can be used to correlate multiple messages
    string correlationId?;
    # JMS destination to which a reply to this message should be sent
    Destination replyTo?;
    # Sender ID, a Solace-specific extension with no standard JMS equivalent
    string senderId?;
    # Additional message properties
    map<Property> properties?;
    # Unique identifier for a JMS message (Only set by the JMS provider)
    string messageId?;
    # Time a message was handed off to a provider to be sent (Only set by the JMS provider)
    int timestamp?;
    # JMS destination of this message (Only set by the JMS provider)
    Destination destination?;
    # Indication of whether this message is being redelivered (Only set by the JMS provider)
    boolean redelivered?;
    # Number of times this message has been delivered (Only set by the JMS provider)
    int deliveryCount?;
    # Message expiration time (Only set by the JMS provider)
    int expiration?;
|};

# Represent the valid value types allowed in JMS message properties.
public type Property boolean|int|byte|float|string;

# Represents the allowed value types for entries in the map content of a JMS MapMessage.
public type Value boolean|int|byte|float|string|byte[];

# A boolean property of Solace message to denote the text payload set in the message is an XML
public const SOLACE_JMS_PROP_ISXML = "JMS_Solace_isXML";

// Internal representation for the Solace message.
type InternalMessage record {|
    string|map<Value>|byte[] payload;
    DeliveryMode deliveryMode?;
    int priority?;
    decimal timeToLive?;
    string messageType?;
    string correlationId?;
    Destination replyTo?;
    string senderId?;
    map<Property> properties?;
    string messageId?;
    int timestamp?;
    Destination destination?;
    boolean redelivered?;
    int deliveryCount?;
    int expiration?;
|};
