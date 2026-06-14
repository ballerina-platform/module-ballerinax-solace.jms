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

# Represents a Solace SMF listener endpoint that can be used to receive messages from
# topic subscriptions (direct) or durable queues (persistent) using native asynchronous delivery.
public isolated class Listener {

    # Initializes a new Solace SMF message listener with the given broker URL and configuration.
    # ```ballerina
    # listener smf:Listener smfListener = check new (
    #     url = "smf://localhost:55554",
    #     auth = {
    #         username: "admin",
    #         password: "admin"
    #     }
    # );
    # ```
    #
    # + url - The Solace broker URL in the format `<scheme>://<host>[:port]`.
    # Supported schemes are `smf`/`tcp` (plain-text) and `smfs`/`tcps` (TLS/SSL)
    # + config - Configurations used when initializing the listener
    # + return - A `smf:Error` if an error occurs or `()` otherwise
    public isolated function init(string url, *ConnectionConfiguration config) returns Error? {
        Error? validated = validateConfigurations(config);
        if validated is Error {
            return error Error(
                string `Error occurred while validating the listener configurations: ${validated.message()}`,
                validated);
        }
        return self.initListener(url, config);
    }

    isolated function initListener(string url, ConnectionConfiguration config) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Listener",
        name: "init"
    } external;

    # Attaches an SMF service to the listener.
    # ```ballerina
    # check smfListener.attach(smfSvc);
    # ```
    #
    # + 'service - Service instance
    # + name - Service name
    # + return - A `smf:Error` if an error occurs or `()` otherwise
    public isolated function attach(Service 'service, string[]|string? name = ()) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Listener"
    } external;

    # Detaches an SMF service from the listener.
    # ```ballerina
    # check smfListener.detach(smfSvc);
    # ```
    #
    # + 'service - Service instance
    # + return - A `smf:Error` if an error occurs or `()` otherwise
    public isolated function detach(Service 'service) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Listener"
    } external;

    # Starts the listener.
    # ```ballerina
    # check smfListener.'start();
    # ```
    #
    # + return - A `smf:Error` if an error occurs or `()` otherwise
    public isolated function 'start() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Listener"
    } external;

    # Gracefully stops the listener.
    # ```ballerina
    # check smfListener.gracefulStop();
    # ```
    #
    # + return - A `smf:Error` if an error occurs or `()` otherwise
    public isolated function gracefulStop() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Listener"
    } external;

    # Immediately stops the listener.
    # ```ballerina
    # check smfListener.immediateStop();
    # ```
    #
    # + return - A `smf:Error` if an error occurs or `()` otherwise
    public isolated function immediateStop() returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Listener"
    } external;
}
