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

# Solace SMF message requester which publishes request messages and awaits the correlated replies,
# implementing the native request-reply messaging pattern.
public isolated client class Requester {

    # Initializes a new Solace SMF message requester with the given broker URL and configuration.
    # ```ballerina
    # smf:Requester requester = check new ("smf://localhost:55554", auth = {
    #     username: "admin",
    #     password: "admin"
    # });
    # ```
    #
    # + url - The Solace broker URL in the format `<scheme>://<host>[:port]`.
    # Supported schemes are `smf`/`tcp` (plain-text) and `smfs`/`tcps` (TLS/SSL)
    # + config - Requester configuration including connection settings
    # + return - A `smf:Error` if initialization fails or else `()`
    public isolated function init(string url, *ConnectionConfiguration config) returns Error? {
        Error? validated = validateConfigurations(config);
        if validated is Error {
            return error Error(
                string `Error occurred while validating the requester configurations: ${validated.message()}`,
                validated);
        }
        return self.externInit(url, config);
    }

    isolated function externInit(string url, ConnectionConfiguration config) returns Error? = @java:Method {
        name: "init",
        'class: "io.ballerina.lib.solace.smf.requestreply.RequesterActions"
    } external;

    # Publishes a request message to the given topic and blocks until the correlated reply
    # arrives or the timeout elapses.
    # ```ballerina
    # smf:Message reply = check requester->request("ping", "service/echo");
    # ```
    #
    # + message - The request message or the request payload to publish
    # + topic - The topic to publish the request to
    # + replyTimeout - The maximum time to wait for the reply, in seconds
    # + return - The reply message, or a `smf:Error` if the request fails or times out
    isolated remote function request(anydata|Message message, string topic, decimal replyTimeout = 30.0)
            returns Message|Error {
        Message smfMessage = message is Message ? message : {payload: message};
        return self.externRequest(toInternalMessage(smfMessage), topic, replyTimeout, Message);
    }

    isolated function externRequest(InternalMessage message, string topic, decimal replyTimeout,
            typedesc<Message> T) returns Message|Error = @java:Method {
        name: "request",
        'class: "io.ballerina.lib.solace.smf.requestreply.RequesterActions"
    } external;

    # Closes the message requester and disconnects from the broker.
    # ```ballerina
    # check requester->close();
    # ```
    #
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function close() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.requestreply.RequesterActions"
    } external;
}
