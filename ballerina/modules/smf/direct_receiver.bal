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

# Solace SMF message receiver which receives messages published with direct (at-most-once) delivery.
# Supports load-balanced shared subscriptions via the `shareName` configuration.
public isolated client class DirectReceiver {

    # Initializes a new Solace SMF direct message receiver with the given broker URL and configuration.
    # ```ballerina
    # smf:DirectReceiver receiver = check new ("smf://localhost:55554",
    #     auth = {username: "admin", password: "admin"},
    #     topicSubscriptions: ["orders/>"]
    # );
    # ```
    #
    # + url - The Solace broker URL in the format `<scheme>://<host>[:port]`.
    # Supported schemes are `smf`/`tcp` (plain-text) and `smfs`/`tcps` (TLS/SSL)
    # + config - Receiver configuration including connection settings and topic subscriptions
    # + return - A `smf:Error` if initialization fails or else `()`
    public isolated function init(string url, *DirectReceiverConfiguration config) returns Error? {
        Error? validated = validateDirectReceiverConfigurations(config);
        if validated is Error {
            return error Error(
                string `Error occurred while validating the receiver configurations: ${validated.message()}`,
                validated);
        }
        return self.externInit(url, config);
    }

    isolated function externInit(string url, DirectReceiverConfiguration config) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.solace.smf.receiver.DirectReceiverActions"
    } external;

    # Receives the next message from the subscribed topics, waiting up to the specified timeout.
    # ```ballerina
    # smf:Message? message = check receiver->receive(5.0);
    # ```
    #
    # + timeout - The maximum time to wait for a message in seconds. Default is 10.0 seconds
    # + T - Optional type description of the expected data type
    # + return - The received message, `()` if no message is available within the timeout,
    # or a `smf:Error` if there is an error
    isolated remote function receive(decimal timeout = 10.0, typedesc<Message> T = <>) returns T|Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.DirectReceiverActions"
    } external;

    # Closes the message receiver and disconnects from the broker.
    # ```ballerina
    # check receiver->close();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.receiver.DirectReceiverActions"
    } external;
}
