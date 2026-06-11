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
