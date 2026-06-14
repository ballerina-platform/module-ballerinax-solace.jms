// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
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

# Solace SMF message receiver which receives messages from a durable queue with guaranteed
# (persistent) delivery, supporting per-message acknowledgement and negative settlement outcomes.
public isolated client class PersistentReceiver {

    # Initializes a new Solace SMF persistent message receiver with the given broker URL and configuration.
    # ```ballerina
    # smf:PersistentReceiver receiver = check new ("smf://localhost:55554",
    #     auth = {username: "admin", password: "admin"},
    #     queueName = "orders"
    # );
    # ```
    #
    # + url - The Solace broker URL in the format `<scheme>://<host>[:port]`.
    # Supported schemes are `smf`/`tcp` (plain-text) and `smfs`/`tcps` (TLS/SSL)
    # + config - Receiver configuration including connection settings, the queue, and settlement behavior
    # + return - A `smf:Error` if initialization fails or else `()`
    public isolated function init(string url, *PersistentReceiverConfiguration config) returns Error? {
        Error? validated = validatePersistentReceiverConfigurations(config);
        if validated is Error {
            return error Error(
                string `Error occurred while validating the receiver configurations: ${validated.message()}`,
                validated);
        }
        return self.externInit(url, config);
    }

    isolated function externInit(string url, PersistentReceiverConfiguration config) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Receives the next message from the queue, waiting up to the specified timeout.
    # ```ballerina
    # smf:Message? message = check receiver->receive(5.0);
    # ```
    #
    # + timeout - The maximum time to wait for a message in seconds. Default is 10.0 seconds
    # + T - Optional type description of the expected data type
    # + return - The received message, `()` if no message is available within the timeout,
    # or a `smf:Error` if there is an error
    isolated remote function receive(decimal timeout = 10.0, typedesc<Message> T = <>) returns T|Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Settles the message with the `ACCEPTED` outcome, removing it from the queue.
    # ```ballerina
    # check receiver->ack(message);
    # ```
    #
    # + message - The message to acknowledge
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function ack(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Settles the message with the `FAILED` outcome. The broker increments the message's delivery
    # count and redelivers it; once the queue's max-redelivery limit is exceeded the message is
    # moved to the dead message queue (if configured). This method requires the receiver to be
    # configured with `negativeSettlementEnabled: true`.
    # ```ballerina
    # check receiver->failed(message);
    # ```
    #
    # + message - The message to settle as failed
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function failed(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Settles the message with the `REJECTED` outcome. The message is removed from the queue
    # and moved to the dead message queue (if configured) without redelivery. This method requires
    # the receiver to be configured with `negativeSettlementEnabled: true`.
    # ```ballerina
    # check receiver->rejected(message);
    # ```
    #
    # + message - The message to settle as rejected
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function rejected(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Pauses message delivery to this receiver.
    # ```ballerina
    # check receiver->pause();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function pause() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Resumes message delivery to this receiver after a pause.
    # ```ballerina
    # check receiver->'resume();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function 'resume() returns Error? = @java:Method {
        name: "resumeReceiver",
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;

    # Closes the message receiver and disconnects from the broker. Received but unacknowledged
    # messages are redelivered by the broker to the next bound receiver.
    # ```ballerina
    # check receiver->close();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.PersistentReceiverActions"
    } external;
}
