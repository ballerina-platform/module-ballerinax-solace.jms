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

import ballerina/jballerina.java;

# Solace SMF message publisher which publishes messages with guaranteed (persistent) delivery.
# Each `publish` call blocks until the broker acknowledges the message, providing a publish receipt.
public isolated client class PersistentPublisher {

    # Initializes a new Solace SMF persistent message publisher with the given broker URL and configuration.
    # ```ballerina
    # smf:PersistentPublisher publisher = check new ("smf://localhost:55554", auth = {
    #     username: "admin",
    #     password: "admin"
    # });
    # ```
    #
    # + url - The Solace broker URL in the format `<scheme>://<host>[:port]`.
    # Supported schemes are `smf`/`tcp` (plain-text) and `smfs`/`tcps` (TLS/SSL).
    # Multiple hosts can be specified as a comma-separated list for failover support
    # + config - Publisher configuration including connection settings and back-pressure behavior
    # + return - A `smf:Error` if initialization fails or else `()`
    public isolated function init(string url, *PublisherConfiguration config) returns Error? {
        Error? validated = validatePublisherConfigurations(config);
        if validated is Error {
            return error Error(
                string `Error occurred while validating the publisher configurations: ${validated.message()}`,
                validated);
        }
        return self.externInit(url, config);
    }

    isolated function externInit(string url, PublisherConfiguration config) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.solace.smf.publisher.PersistentPublisherActions"
    } external;

    # Publishes a message to the given topic with guaranteed delivery and awaits the publish
    # receipt from the broker. To deliver the message to a queue, add a topic subscription
    # to the queue on the broker and publish to the subscribed topic.
    # ```ballerina
    # check publisher->publish("Hello, World!", "orders/retail/usa");
    # ```
    #
    # + message - The message or the message payload to publish
    # + topic - The topic to publish the message to. Topics support multi-level
    # hierarchies using '/' as a delimiter (e.g., `orders/retail/usa`)
    # + timeout - The maximum time to wait for the publish receipt, in seconds
    # + return - A `smf:Error` if the broker rejects the message, the receipt times out,
    # or another error occurs, or else `()`
    isolated remote function publish(anydata|Message message, string topic, decimal timeout = 30.0)
            returns Error? {
        Message smfMessage = message is Message ? message : {payload: message};
        return self.externPublish(toInternalMessage(smfMessage), topic, timeout);
    }

    isolated function externPublish(InternalMessage message, string topic, decimal timeout)
            returns Error? = @java:Method {
        name: "publish",
        'class: "io.ballerina.lib.solace.smf.publisher.PersistentPublisherActions"
    } external;

    # Closes the message publisher and disconnects from the broker.
    # Buffered messages are flushed within a grace period before the publisher terminates.
    # ```ballerina
    # check publisher->close();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.publisher.PersistentPublisherActions"
    } external;
}
