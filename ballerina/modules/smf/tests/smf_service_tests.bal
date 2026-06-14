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

import ballerina/lang.runtime;
import ballerina/test;

const string SERVICE_TEST_TOPIC = "smf/test/service";
const string SERVICE_TEST_QUEUE = "smf-service-queue";
const string AUTOACK_TEST_TOPIC = "smf/test/autoack";
const string AUTOACK_TEST_QUEUE = "smf-autoack-queue";
const string DIRECT_SERVICE_TOPIC = "smf/test/direct-service";

isolated string? queueServicePayload = ();
isolated string? autoAckServicePayload = ();
isolated string? directServicePayload = ();

isolated function awaitPayload(isolated function () returns string? accessor) returns string? {
    int attempts = 0;
    while attempts < 60 {
        string? payload = accessor();
        if payload is string {
            return payload;
        }
        runtime:sleep(0.5);
        attempts += 1;
    }
    return ();
}

@test:Config {
    groups: ["smfService"]
}
function testQueueServiceWithCallerAck() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service queueService = @ServiceConfig {
        queueName: SERVICE_TEST_QUEUE
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            anydata payload = message.payload;
            if payload is string {
                lock {
                    queueServicePayload = payload;
                }
            }
            check caller->ack(message);
        }
    };
    check smfListener.attach(queueService);
    check smfListener.'start();

    check publishPersistent("queue service test", SERVICE_TEST_TOPIC);

    string? received = awaitPayload(isolated function() returns string? {
        lock {
            return queueServicePayload;
        }
    });
    check smfListener.gracefulStop();
    test:assertEquals(received, "queue service test");
}

@test:Config {
    groups: ["smfService"]
}
function testQueueServiceWithAutoAck() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service autoAckService = @ServiceConfig {
        queueName: AUTOACK_TEST_QUEUE,
        autoAck: true
    } service object {
        remote function onMessage(Message message) returns error? {
            anydata payload = message.payload;
            if payload is string {
                lock {
                    autoAckServicePayload = payload;
                }
            }
        }
    };
    check smfListener.attach(autoAckService);
    check smfListener.'start();

    check publishPersistent("auto-ack service test", AUTOACK_TEST_TOPIC);

    string? received = awaitPayload(isolated function() returns string? {
        lock {
            return autoAckServicePayload;
        }
    });
    check smfListener.gracefulStop();
    test:assertEquals(received, "auto-ack service test");

    // The auto-acknowledged message must not remain on the queue
    PersistentReceiver verifier = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = AUTOACK_TEST_QUEUE
    );
    Message? leftover = check verifier->receive(3.0);
    check verifier->close();
    test:assertTrue(leftover is (), "Auto-acknowledged message should not remain on the queue");
}

@test:Config {
    groups: ["smfService"]
}
function testDirectTopicService() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service directService = @ServiceConfig {
        topicSubscriptions: [DIRECT_SERVICE_TOPIC]
    } service object {
        remote function onMessage(Message message) returns error? {
            anydata payload = message.payload;
            if payload is string {
                lock {
                    directServicePayload = payload;
                }
            }
        }
    };
    check smfListener.attach(directService);
    check smfListener.'start();

    DirectPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    // Direct delivery is at-most-once: a publish racing the subscription propagation is silently
    // dropped, so publish repeatedly until the service observes a message
    string? received = ();
    foreach int attempt in 0 ..< 15 {
        check publisher->publish("direct service test", DIRECT_SERVICE_TOPIC);
        runtime:sleep(1);
        lock {
            received = directServicePayload;
        }
        if received is string {
            break;
        }
    }
    check publisher->close();
    check smfListener.gracefulStop();
    test:assertEquals(received, "direct service test");
}

@test:Config {
    groups: ["smfService", "smfServiceValidation"]
}
function testServiceWithoutAnnotation() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service invalidService = service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    Error? result = smfListener.attach(invalidService);
    check smfListener.gracefulStop();
    if result !is Error {
        test:assertFail("Expected an error when attaching a service without the ServiceConfig annotation");
    }
    test:assertTrue(result.message().includes("Service configuration annotation is required"));
}

@test:Config {
    groups: ["smfService", "smfServiceValidation"]
}
function testDirectServiceWithCallerRejected() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service invalidService = @ServiceConfig {
        topicSubscriptions: [DIRECT_SERVICE_TOPIC]
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
        }
    };
    Error? result = smfListener.attach(invalidService);
    check smfListener.gracefulStop();
    if result !is Error {
        test:assertFail("Expected an error when a direct subscription service declares an smf:Caller parameter");
    }
    test:assertTrue(result.message().includes("not supported for direct topic subscriptions"));
}

@test:Config {
    groups: ["smfService", "smfServiceValidation"]
}
function testServiceWithInvalidRemoteMethod() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service invalidService = @ServiceConfig {
        queueName: SERVICE_TEST_QUEUE
    } service object {
        remote function onRequest(Message message) returns error? {
        }
    };
    Error? result = smfListener.attach(invalidService);
    check smfListener.gracefulStop();
    if result !is Error {
        test:assertFail("Expected an error when a service declares an unsupported remote method");
    }
    test:assertTrue(result.message().includes("Invalid remote method name"));
}

@test:Config {
    groups: ["smfService", "smfServiceValidation"]
}
function testQueueServiceWithoutCallerOrAutoAckRejected() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    // Queue subscription, autoAck defaults to false, and onMessage omits smf:Caller — there is no
    // way to acknowledge messages, so attach must reject it rather than silently never settling.
    Service invalidService = @ServiceConfig {
        queueName: SERVICE_TEST_QUEUE
    } service object {
        remote function onMessage(Message message) returns error? {
        }
    };
    Error? result = smfListener.attach(invalidService);
    check smfListener.gracefulStop();
    if result !is Error {
        test:assertFail("Expected an error when a queue service omits smf:Caller without autoAck");
    }
    test:assertTrue(result.message().includes("must declare an 'smf:Caller'"));
}
