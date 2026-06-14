// Copyright (c) 2026 WSO2 LLC. (http://www.wso2.com).
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

import ballerina/time;
import ballerinax/solace;

# SSL protocol version 3.0
public const SSL_V3 = "SSLv3";
# TLS protocol version 1.0
public const TLS_V1 = "TLSv1";
# TLS protocol version 1.1
public const TLS_V11 = "TLSv1_1";
# TLS protocol version 1.2
public const TLS_V12 = "TLSv1_2";
# TLS protocol version 1.3
public const TLS_V13 = "TLSv1_3";

# Represents the SSL/TLS protocol versions which can be excluded from the connection.
public type ExcludedProtocol SSL_V3|TLS_V1|TLS_V11|TLS_V12|TLS_V13;

# Represents the SSL/TLS configuration for secure SMF connections to a Solace broker.
public type SecureSocket record {|
    # The trust store configuration containing trusted CA certificates
    solace:TrustStore trustStore?;
    # The key store configuration containing the client's private key and certificate.
    # When configured, enables client certificate authentication
    solace:KeyStore keyStore?;
    # The list of SSL/TLS protocol versions to exclude from the connection.
    # It is recommended to exclude protocols older than TLS 1.2 for security
    ExcludedProtocol[] excludedProtocols = [];
    # The list of cipher suites to enable for the connection.
    # If not specified, the default cipher suites for the JVM are used
    solace:SslCipherSuite[] cipherSuites?;
    # The certificate validation settings
    record {|
        # Enable certificate validation
        boolean enabled = true;
        # Specifies whether to validate the certificate's expiration date
        boolean validateDate = true;
        # Specifies whether to validate that the certificate's common name matches the broker hostname
        boolean validateHost = true;
    |} validation = {};
|};

# Represents the common connection configuration for Solace SMF clients.
public type ConnectionConfiguration record {
    # The name of the message VPN to connect to
    string messageVpn = "default";
    # The authentication configuration. Supports basic authentication, Kerberos, and OAuth2.
    # For client certificate authentication, configure the `secureSocket.keyStore` field
    solace:BasicAuthConfig|solace:KerberosConfig|solace:OAuth2Config auth?;
    # The SSL/TLS configuration for secure connections
    SecureSocket secureSocket?;
    # The retry configuration for connection and reconnection attempts
    solace:RetryConfig retryConfig?;
    # The client name used to identify the client on the broker.
    # If not specified, a unique client name is auto-generated
    string clientName?;
    # A description for the application client
    string applicationDescription?;
    # The maximum amount of time (in seconds) permitted for a connection attempt
    decimal connectTimeout = 30.0;
    # The configuration to enable and specify the ZLIB compression level.
    # Valid range is 0-9, where 0 means no compression. Higher values provide better compression at slower throughput
    int compressionLevel = 0;
};

# Back-pressure strategy: block the publish call until buffer space becomes available
public const WAIT_WHEN_FULL = "WAIT_WHEN_FULL";
# Back-pressure strategy: fail the publish call immediately when the buffer is full
public const REJECT_WHEN_FULL = "REJECT_WHEN_FULL";
# Back-pressure strategy: use an unbounded internal buffer
public const ELASTIC = "ELASTIC";

# Represents the supported publisher back-pressure strategies.
public type BackPressureStrategy WAIT_WHEN_FULL|REJECT_WHEN_FULL|ELASTIC;

# Represents the back-pressure configuration for SMF message publishers.
public type BackPressureConfig record {|
    # The back-pressure strategy to apply when the publisher's internal buffer is full
    BackPressureStrategy strategy = ELASTIC;
    # The capacity of the publisher's internal buffer. Ignored when the strategy is `ELASTIC`
    int bufferCapacity = 1024;
|};

# Represents the configuration for Solace SMF message publishers.
public type PublisherConfiguration record {
    *ConnectionConfiguration;
    # The back-pressure configuration for the publisher
    BackPressureConfig backPressure = {};
};

# Endpoint resources missing on the broker are not created; receiver start fails if they are absent
public const DO_NOT_CREATE = "DO_NOT_CREATE";
# Endpoint resources missing on the broker are created when the receiver starts
public const CREATE_ON_START = "CREATE_ON_START";

# Represents the strategies for provisioning missing broker resources (queues and endpoints).
# This is the SMF counterpart of the JMS surface's `enableDynamicDurables` configuration.
public type MissingResourcesStrategy DO_NOT_CREATE|CREATE_ON_START;

# Represents the configuration for the Solace SMF direct message receiver.
public type DirectReceiverConfiguration record {
    *ConnectionConfiguration;
    # The topic subscriptions to receive messages from. Topics support wildcard
    # subscriptions and multi-level hierarchies using '/' as a delimiter
    string[] topicSubscriptions;
    # The share name for a shared subscription. When set, multiple receivers using the
    # same share name receive messages from the matching topics in a load-balanced manner
    string shareName?;
};

# Replays all messages retained in the replay log for the queue.
public const ALL_MESSAGES = "ALL_MESSAGES";

# Represents a replay strategy which replays messages logged on or after the given time.
public type TimeBasedReplay record {|
    # The time to start the replay from
    time:Utc fromTime;
|};

# Represents a replay strategy which replays messages logged after the message with the
# given replication group message id.
public type ReplicationGroupIdReplay record {|
    # The replication group message id after which messages are replayed.
    # Obtain it from the `replicationGroupMessageId` field of a received message
    string afterMessageId;
|};

# Represents the supported message replay strategies. Message replay requires a replay log
# to be provisioned on the broker, and is not supported with partitioned queues or under replication.
public type ReplayStrategy ALL_MESSAGES|TimeBasedReplay|ReplicationGroupIdReplay;

# Represents the configuration for the Solace SMF persistent message receiver.
public type PersistentReceiverConfiguration record {
    *ConnectionConfiguration;
    # The name of the durable queue to consume messages from
    string queueName;
    # The message replay strategy. When set, the broker redelivers eligible messages from
    # the replay log to this receiver before live messages
    ReplayStrategy replayStrategy?;
    # Additional topic subscriptions to add to the queue (programmatic topic-to-queue mapping)
    string[] topicSubscriptions = [];
    # Only messages with properties matching the message selector expression are delivered.
    # If this value is not set, all messages in the queue will be delivered
    string messageSelector?;
    # The strategy for provisioning the queue on the broker when it does not exist
    MissingResourcesStrategy missingResourcesStrategy = DO_NOT_CREATE;
    # When `true`, messages are automatically acknowledged by the underlying API after a successful
    # receive. When `false` (default), messages must be explicitly acknowledged via `ack()`;
    # unacknowledged messages are redelivered after the receiver flow reconnects
    boolean autoAck = false;
    # Enables the negative settlement outcomes `FAILED` and `REJECTED` on this receiver
    # (requires Solace broker 10.2.1 or later). Mutually exclusive with `autoAck`
    boolean negativeSettlementEnabled = false;
};

# Represents the SMF service configuration for a direct (at-most-once) topic subscription.
public type DirectSubscriptionConfig record {|
    # The topic subscriptions to receive messages from
    string[] topicSubscriptions;
    # The share name for a load-balanced shared subscription
    string shareName?;
|};

# Represents the SMF service configuration for a persistent queue subscription.
public type QueueSubscriptionConfig record {|
    # The name of the durable queue to consume messages from
    string queueName;
    # Additional topic subscriptions to add to the queue (programmatic topic-to-queue mapping)
    string[] topicSubscriptions = [];
    # Only messages with properties matching the message selector expression are delivered
    string messageSelector?;
    # The strategy for provisioning the queue on the broker when it does not exist
    MissingResourcesStrategy missingResourcesStrategy = DO_NOT_CREATE;
    # When `true`, messages are acknowledged automatically after the `onMessage` handler returns
    # successfully. When `false` (default), messages must be acknowledged via the `smf:Caller`
    boolean autoAck = false;
    # Enables the negative settlement outcomes `failed()` and `rejected()` on the `smf:Caller`
    # (requires Solace broker 10.2.1 or later). Mutually exclusive with `autoAck`
    boolean negativeSettlementEnabled = false;
|};

# Represents the configuration for the Solace SMF request-reply message replier.
public type ReplierConfiguration record {
    *ConnectionConfiguration;
    # The topic subscription to receive request messages from
    string topicSubscription;
    # The share name for a load-balanced shared subscription
    string shareName?;
};

# The service configuration type for the `smf:Service`.
public type ServiceConfiguration DirectSubscriptionConfig|QueueSubscriptionConfig;

# Annotation to configure the `smf:Service`.
public annotation ServiceConfiguration ServiceConfig on service;

# The Solace SMF service type.
public type Service distinct service object {
    // remote function onMessage(smf:Message message, smf:Caller caller) returns error?;
};

# Represent the message used to send and receive content from the Solace broker over SMF.
public type Message record {|
    # Message payload
    anydata payload;
    # Id which can be used to correlate multiple messages
    string correlationId?;
    # Additional message properties. The underlying Solace messaging API supports
    # string-typed user properties only
    map<string> properties?;
    # Application-provided message identifier
    string applicationMessageId?;
    # Application-provided message type identifier
    string applicationMessageType?;
    # Message priority level (0-255, higher value means higher priority)
    int priority?;
    # The number of seconds the message is kept by the broker before being discarded
    # or moved to a dead message queue. Only applies to guaranteed (persistent) delivery
    decimal timeToLive?;
    # Specifies whether the message is eligible to be moved to a dead message queue
    # when it cannot be delivered. Only applies to guaranteed (persistent) delivery
    boolean dmqEligible = true;
    # Application-provided sequence number
    int sequenceNumber?;
    # Application-provided sender identifier
    string senderId?;
    # Indication of whether this message is being redelivered (Only set by the broker)
    boolean redelivered?;
    # The destination name this message was received from (Only set by the broker)
    string destinationName?;
    # The replication group message id usable for message replay (Only set by the broker)
    string replicationGroupMessageId?;
    # Message expiration time as a timestamp in milliseconds (Only set by the broker)
    int expiration?;
    # Time the message was received by the broker, in milliseconds (Only set by the broker)
    int timestamp?;
    # Time the message was sent by the publisher, in milliseconds (Only set by the broker)
    int senderTimestamp?;
|};

// Internal representation for the Solace SMF message.
type InternalMessage record {|
    string|byte[] payload;
    string correlationId?;
    map<string> properties?;
    string applicationMessageId?;
    string applicationMessageType?;
    int priority?;
    decimal timeToLive?;
    boolean dmqEligible = true;
    int sequenceNumber?;
    string senderId?;
|};
