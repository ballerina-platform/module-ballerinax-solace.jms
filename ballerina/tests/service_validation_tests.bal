// Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
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

import ballerina/test;

@test:Config {
    groups: ["service", "validations"]
}
isolated function testDetachFailure() returns error? {
    Service consumerSvc = @ServiceConfig {
        ackMode: CLIENT_ACKNOWLEDGE,
        queueName: "test.svc.attach"
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
        }
    };
    Error? result = solaceListener.detach(consumerSvc);
    test:assertTrue(result is Error);
    if result is Error {
        test:assertEquals(
                result.message(),
                "Could not find the native Solace message receiver",
                "Invalid error message");
    }
}

@test:Config {
    groups: ["service", "validations"]
}
isolated function testListenerAttachTransactedRequiresGuaranteedDelivery() returns error? {
    Listener msgListener = check new Listener(BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    Service consumerSvc = @ServiceConfig {
        ackMode: SESSION_TRANSACTED,
        queueName: "test-svc-attach"
    } service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    Error? result = msgListener.attach(consumerSvc);
    test:assertTrue(result is Error,
            "Expected validation error when ackMode is SESSION_TRANSACTED with directTransport left at default true");
    if result is Error {
        test:assertTrue(result.message().toLowerAscii().includes("directtransport"),
                "Error message should mention directTransport");
    }
    check msgListener.immediateStop();
}
