// Copyright (c) 2026 WSO2 LLC. (http://www.wso2.org).
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

import ballerinax/solace.jms as jms;

jms:Service missingAnnotation = service object {
    remote function onMessage(jms:Message message) returns error? {}
};

jms:Service resourceMethod = @jms:ServiceConfig {queueName: "q"} service object {
    resource function get status() returns string => "ok";
    remote function onMessage(jms:Message message) returns error? {}
};

jms:Service missingOnMessage = @jms:ServiceConfig {queueName: "q"} service object {
};

jms:Service unsupportedMethod = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onEvent(jms:Message message) returns error? {}
};

jms:Service invalidParameterCount = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage() returns error? {}
};

jms:Service invalidMessageType = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(string payload) returns error? {}
};

jms:Service invalidCallerPosition = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message, jms:Message second) returns error? {}
};

jms:Service invalidOnError = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message) returns error? {}
    remote function onError(jms:Message err) returns error? {}
};

jms:Service invalidReturn = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(jms:Message message) returns string => "invalid";
};

type StringMessage record {|
    *jms:Message;
    string payload;
|};

jms:Service validService = @jms:ServiceConfig {queueName: "q"} service object {
    remote function onMessage(StringMessage message, jms:Caller caller) returns jms:Error? {}
    remote function onError(jms:Error err) returns error? {}
};
