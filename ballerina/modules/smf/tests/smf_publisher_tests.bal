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

import ballerina/test;
import ballerinax/solace;

const string BROKER_URL = "smf://localhost:55554";
const string BROKER_USERNAME = "admin";
const string BROKER_PASSWORD = "admin";

// Queue `smf-test-queue` is provisioned by init-solace.sh with a subscription to this topic
const string PERSISTENT_TEST_TOPIC = "smf/test/persistent";
const string PERSISTENT_TEST_QUEUE = "smf-test-queue";
const string DIRECT_TEST_TOPIC = "smf/test/direct";

@test:Config {
    groups: ["smfPublisher", "smfPersistent"]
}
isolated function testPersistentPublishToQueueSubscription() returns error? {
    PersistentPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    check publisher->publish("Hello from SMF persistent publisher", PERSISTENT_TEST_TOPIC);
    check publisher->close();

    // Verify via the JMS surface that the message landed on the queue mapped to the topic
    solace:MessageConsumer consumer = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        directTransport = false,
        subscriptionConfig = {queueName: PERSISTENT_TEST_QUEUE}
    );
    solace:Message? received = check consumer->receive(15.0);
    check consumer->close();
    if received is () {
        test:assertFail("Expected a message on the queue mapped to the persistent test topic");
    }
    test:assertEquals(received.payload, "Hello from SMF persistent publisher");
}

@test:Config {
    groups: ["smfPublisher", "smfPersistent"],
    dependsOn: [testPersistentPublishToQueueSubscription]
}
isolated function testPersistentPublishWithMessageFields() returns error? {
    PersistentPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Message message = {
        payload: "message with fields",
        correlationId: "smf-correlation-1",
        properties: {"orderType": "retail"},
        applicationMessageId: "smf-msg-1",
        priority: 4
    };
    check publisher->publish(message, PERSISTENT_TEST_TOPIC);
    check publisher->close();

    solace:MessageConsumer consumer = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        directTransport = false,
        subscriptionConfig = {queueName: PERSISTENT_TEST_QUEUE}
    );
    solace:Message? received = check consumer->receive(15.0);
    check consumer->close();
    if received is () {
        test:assertFail("Expected a message on the queue mapped to the persistent test topic");
    }
    test:assertEquals(received.payload, "message with fields");
    test:assertEquals(received.correlationId, "smf-correlation-1");
    map<solace:Property>? properties = received.properties;
    if properties is () {
        test:assertFail("Expected message properties to be present");
    }
    test:assertEquals(properties["orderType"], "retail");
}

@test:Config {
    groups: ["smfPublisher", "smfPersistent"],
    dependsOn: [testPersistentPublishWithMessageFields]
}
isolated function testPersistentPublishBinaryAndJsonPayloads() returns error? {
    PersistentPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    byte[] binaryPayload = "binary-payload".toBytes();
    check publisher->publish(binaryPayload, PERSISTENT_TEST_TOPIC);
    json jsonPayload = {orderId: 1234, region: "usa"};
    check publisher->publish(jsonPayload, PERSISTENT_TEST_TOPIC);
    check publisher->close();

    solace:MessageConsumer consumer = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        directTransport = false,
        subscriptionConfig = {queueName: PERSISTENT_TEST_QUEUE}
    );
    solace:Message? first = check consumer->receive(15.0);
    solace:Message? second = check consumer->receive(15.0);
    check consumer->close();
    if first is () || second is () {
        test:assertFail("Expected two messages on the queue mapped to the persistent test topic");
    }
    test:assertEquals(first.payload, binaryPayload);
    test:assertEquals(second.payload, jsonPayload.toJsonString().toBytes());
}

@test:Config {
    groups: ["smfPublisher", "smfDirect"]
}
isolated function testDirectPublishToTopic() returns error? {
    // Subscribe via the JMS surface first so the direct message has a live subscriber
    solace:MessageConsumer consumer = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        subscriptionConfig = {topicName: DIRECT_TEST_TOPIC}
    );

    DirectPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    check publisher->publish("Hello from SMF direct publisher", DIRECT_TEST_TOPIC);

    solace:Message? received = check consumer->receive(15.0);
    check publisher->close();
    check consumer->close();
    if received is () {
        test:assertFail("Expected a direct message on the test topic");
    }
    test:assertEquals(received.payload, "Hello from SMF direct publisher");
}

@test:Config {
    groups: ["smfPublisher", "smfDirect"]
}
isolated function testDirectPublishWithBackPressureConfig() returns error? {
    DirectPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        backPressure = {strategy: REJECT_WHEN_FULL, bufferCapacity: 64}
    );
    check publisher->publish("back-pressure configured publish", DIRECT_TEST_TOPIC);
    check publisher->close();
}

@test:Config {
    groups: ["smfPublisher", "smfValidation"]
}
isolated function testPublisherInvalidCompressionLevel() returns error? {
    PersistentPublisher|Error publisher = new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        compressionLevel = 10
    );
    if publisher !is Error {
        test:assertFail("Expected a validation error for an out-of-range compression level");
    }
    test:assertTrue(publisher.message().includes("ZLIB compression level cannot exceed 9"));
}

@test:Config {
    groups: ["smfPublisher", "smfValidation"]
}
isolated function testPublisherInvalidBackPressureCapacity() returns error? {
    DirectPublisher|Error publisher = new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        backPressure = {strategy: WAIT_WHEN_FULL, bufferCapacity: 0}
    );
    if publisher !is Error {
        test:assertFail("Expected a validation error for an invalid back-pressure buffer capacity");
    }
    test:assertTrue(publisher.message().includes("Back-pressure buffer capacity must be at least 1"));
}

@test:Config {
    groups: ["smfPublisher", "smfValidation"]
}
isolated function testPublisherConnectionFailure() returns error? {
    PersistentPublisher|Error publisher = new ("smf://localhost:49999",
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        connectTimeout = 3.0
    );
    if publisher !is Error {
        test:assertFail("Expected a connection error for an unreachable broker host");
    }
}
