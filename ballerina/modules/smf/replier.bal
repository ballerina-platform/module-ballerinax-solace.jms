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

# Solace SMF message replier which receives request messages from a topic subscription and
# sends correlated replies, implementing the native request-reply messaging pattern.
public isolated client class Replier {

    # Initializes a new Solace SMF message replier with the given broker URL and configuration.
    # ```ballerina
    # smf:Replier replier = check new ("smf://localhost:55554",
    #     auth = {username: "admin", password: "admin"},
    #     topicSubscription = "service/echo"
    # );
    # ```
    #
    # + url - The Solace broker URL in the format `<scheme>://<host>[:port]`.
    # Supported schemes are `smf`/`tcp` (plain-text) and `smfs`/`tcps` (TLS/SSL)
    # + config - Replier configuration including connection settings and the request topic subscription
    # + return - A `smf:Error` if initialization fails or else `()`
    public isolated function init(string url, *ReplierConfiguration config) returns Error? {
        Error? validated = validateConfigurations(config);
        if validated is Error {
            return error Error(
                string `Error occurred while validating the replier configurations: ${validated.message()}`,
                validated);
        }
        return self.externInit(url, config);
    }

    isolated function externInit(string url, ReplierConfiguration config) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.solace.smf.requestreply.ReplierActions"
    } external;

    # Receives the next request message, waiting up to the specified timeout. Use `reply` with
    # the received message to send the correlated response.
    # ```ballerina
    # smf:Message? request = check replier->receive(5.0);
    # ```
    #
    # + timeout - The maximum time to wait for a request in seconds. Default is 10.0 seconds
    # + T - Optional type description of the expected request data type
    # + return - The received request message, `()` if no request arrives within the timeout,
    # or a `smf:Error` if there is an error
    isolated remote function receive(decimal timeout = 10.0, typedesc<Message> T = <>) returns T|Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.requestreply.ReplierActions"
    } external;

    # Sends a reply for a previously received request message.
    # ```ballerina
    # check replier->reply(request, "pong");
    # ```
    #
    # + request - The received request message to reply to
    # + response - The reply message or the reply payload
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function reply(Message request, anydata|Message response) returns Error? {
        Message smfMessage = response is Message ? response : {payload: response};
        return self.externReply(request, toInternalMessage(smfMessage));
    }

    isolated function externReply(Message request, InternalMessage response) returns Error? = @java:Method {
        name: "reply",
        'class: "io.ballerina.lib.solace.smf.requestreply.ReplierActions"
    } external;

    # Closes the message replier and disconnects from the broker.
    # ```ballerina
    # check replier->close();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.requestreply.ReplierActions"
    } external;
}
