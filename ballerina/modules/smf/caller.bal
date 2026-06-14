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

# Represents a Solace SMF caller, which can be used to settle messages received by an `smf:Service`
# attached to a persistent queue subscription.
public isolated client class Caller {

    # Settles the message with the `ACCEPTED` outcome, removing it from the queue.
    # ```ballerina
    # check caller->ack(message);
    # ```
    #
    # + message - The message to acknowledge
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function ack(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Caller"
    } external;

    # Settles the message with the `FAILED` outcome. The broker increments the message's delivery
    # count and redelivers it; once the queue's max-redelivery limit is exceeded the message is
    # moved to the dead message queue (if configured). Requires the service to be configured with
    # `negativeSettlementEnabled: true`.
    # ```ballerina
    # check caller->failed(message);
    # ```
    #
    # + message - The message to settle as failed
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function failed(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Caller"
    } external;

    # Settles the message with the `REJECTED` outcome. The message is removed from the queue and
    # moved to the dead message queue (if configured) without redelivery. Requires the service to
    # be configured with `negativeSettlementEnabled: true`.
    # ```ballerina
    # check caller->rejected(message);
    # ```
    #
    # + message - The message to settle as rejected
    # + return - A `smf:Error` if there is an error or else `()`
    isolated remote function rejected(Message message) returns Error? = @java:Method {
        'class: "io.ballerina.lib.solace.smf.listener.Caller"
    } external;
}
