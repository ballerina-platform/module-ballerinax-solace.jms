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

import ballerina/test;

// Queues are provisioned by init-solace.sh with subscriptions to the corresponding topics
const string RECEIVER_TEST_TOPIC = "smf/test/receiver";
const string RECEIVER_TEST_QUEUE = "smf-receiver-queue";
const string SETTLEMENT_TEST_TOPIC = "smf/test/settlement";
const string SETTLEMENT_TEST_QUEUE = "smf-settlement-queue";
const string REJECTED_TEST_TOPIC = "smf/test/rejected";
const string REJECTED_TEST_QUEUE = "smf-rejected-queue";
const string DIRECT_RECEIVER_TOPIC = "smf/test/direct-receiver";
const string SHARED_SUBSCRIPTION_TOPIC = "smf/test/shared";
const string PROVISIONED_TOPIC = "smf/test/provisioned";
const string PROVISIONED_QUEUE = "smf-provisioned-queue";

type Order record {|
    int orderId;
    string region;
|};

type OrderMessage record {|
    *Message;
    Order payload;
|};

isolated function publishPersistent(string payload, string topic) returns error? {
    PersistentPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    check publisher->publish(payload, topic);
    check publisher->close();
}

@test:Config {
    groups: ["smfReceiver", "smfPersistentReceiver"]
}
isolated function testPersistentReceiverReceiveAndAck() returns error? {
    check publishPersistent("persistent receiver test", RECEIVER_TEST_TOPIC);

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE
    );
    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the receiver test queue");
    }
    test:assertEquals(message.payload, "persistent receiver test");
    test:assertEquals(message.destinationName, RECEIVER_TEST_TOPIC);
    check receiver->ack(message);
    check receiver->close();

    // The acknowledged message must not be redelivered to a new receiver
    PersistentReceiver verifier = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE
    );
    Message? redelivered = check verifier->receive(3.0);
    check verifier->close();
    test:assertTrue(redelivered is (), "Acknowledged message should not be redelivered");
}

@test:Config {
    groups: ["smfReceiver", "smfPersistentReceiver"],
    dependsOn: [testPersistentReceiverReceiveAndAck]
}
isolated function testPersistentReceiverUnackedMessageRedelivery() returns error? {
    check publishPersistent("unacked message", RECEIVER_TEST_TOPIC);

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE
    );
    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the receiver test queue");
    }
    // Close without acknowledging - the broker must redeliver to the next bound receiver
    check receiver->close();

    PersistentReceiver redeliveryReceiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE
    );
    Message? redelivered = check redeliveryReceiver->receive(15.0);
    if redelivered is () {
        check redeliveryReceiver->close();
        test:assertFail("Expected the unacknowledged message to be redelivered");
    }
    test:assertEquals(redelivered.payload, "unacked message");
    test:assertEquals(redelivered.redelivered, true);
    check redeliveryReceiver->ack(redelivered);
    check redeliveryReceiver->close();
}

@test:Config {
    groups: ["smfReceiver", "smfSettlement"]
}
isolated function testSettlementFailedOutcomeTriggersRedelivery() returns error? {
    check publishPersistent("failed settlement test", SETTLEMENT_TEST_TOPIC);

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = SETTLEMENT_TEST_QUEUE,
        negativeSettlementEnabled = true
    );
    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the settlement test queue");
    }
    check receiver->failed(message);

    // FAILED increments the delivery count and the broker redelivers the message
    Message? redelivered = check receiver->receive(15.0);
    if redelivered is () {
        check receiver->close();
        test:assertFail("Expected the failed message to be redelivered");
    }
    test:assertEquals(redelivered.payload, "failed settlement test");
    test:assertEquals(redelivered.redelivered, true);
    check receiver->ack(redelivered);
    check receiver->close();
}

@test:Config {
    groups: ["smfReceiver", "smfSettlement"]
}
isolated function testSettlementRejectedOutcomeDiscardsMessage() returns error? {
    check publishPersistent("rejected settlement test", REJECTED_TEST_TOPIC);

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = REJECTED_TEST_QUEUE,
        negativeSettlementEnabled = true
    );
    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the rejected test queue");
    }
    check receiver->rejected(message);

    // REJECTED removes the message from the queue without redelivery
    Message? next = check receiver->receive(3.0);
    check receiver->close();
    test:assertTrue(next is (), "Rejected message should not be redelivered");
}

@test:Config {
    groups: ["smfReceiver", "smfSettlement"]
}
isolated function testNegativeSettlementDisabledError() returns error? {
    check publishPersistent("settlement disabled test", RECEIVER_TEST_TOPIC);

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE
    );
    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the receiver test queue");
    }
    Error? failedResult = receiver->failed(message);
    test:assertTrue(failedResult is Error,
        "Expected an error when using 'failed' without enabling negative settlement");
    check receiver->ack(message);
    check receiver->close();
}

@test:Config {
    groups: ["smfReceiver", "smfDirectReceiver"]
}
isolated function testDirectReceiverReceive() returns error? {
    DirectReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        topicSubscriptions = [DIRECT_RECEIVER_TOPIC]
    );

    DirectPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    check publisher->publish("direct receiver test", DIRECT_RECEIVER_TOPIC);

    Message? message = check receiver->receive(15.0);
    check publisher->close();
    check receiver->close();
    if message is () {
        test:assertFail("Expected a direct message on the subscribed topic");
    }
    test:assertEquals(message.payload, "direct receiver test");
}

@test:Config {
    groups: ["smfReceiver", "smfDirectReceiver"]
}
isolated function testDirectSharedSubscription() returns error? {
    DirectReceiver receiver1 = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        topicSubscriptions = [SHARED_SUBSCRIPTION_TOPIC],
        shareName = "smf-test-share"
    );
    DirectReceiver receiver2 = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        topicSubscriptions = [SHARED_SUBSCRIPTION_TOPIC],
        shareName = "smf-test-share"
    );

    DirectPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    int messageCount = 4;
    foreach int i in 0 ..< messageCount {
        check publisher->publish(string `shared message ${i}`, SHARED_SUBSCRIPTION_TOPIC);
    }

    // With a shared subscription each message is delivered to exactly one member of the group
    int received = 0;
    foreach int i in 0 ..< messageCount {
        Message? fromFirst = check receiver1->receive(2.0);
        if fromFirst !is () {
            received += 1;
        }
        Message? fromSecond = check receiver2->receive(2.0);
        if fromSecond !is () {
            received += 1;
        }
        if received >= messageCount {
            break;
        }
    }
    check publisher->close();
    check receiver1->close();
    check receiver2->close();
    test:assertEquals(received, messageCount,
        "Each shared-subscription message must be delivered exactly once across the group");
}

@test:Config {
    groups: ["smfReceiver", "smfProvisioning"]
}
isolated function testPersistentReceiverMissingResourceProvisioning() returns error? {
    // The queue is NOT pre-provisioned; CREATE_ON_START must create it along with the subscription
    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = PROVISIONED_QUEUE,
        topicSubscriptions = [PROVISIONED_TOPIC],
        missingResourcesStrategy = CREATE_ON_START
    );

    check publishPersistent("provisioned queue test", PROVISIONED_TOPIC);

    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the dynamically provisioned queue");
    }
    test:assertEquals(message.payload, "provisioned queue test");
    check receiver->ack(message);
    check receiver->close();
}

@test:Config {
    groups: ["smfReceiver", "smfDataBinding"]
}
isolated function testPersistentReceiverDataBinding() returns error? {
    Order orderPayload = {orderId: 7, region: "emea"};
    PersistentPublisher publisher = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    check publisher->publish(orderPayload, RECEIVER_TEST_TOPIC);
    check publisher->close();

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE
    );
    OrderMessage? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message on the receiver test queue");
    }
    test:assertEquals(message.payload, orderPayload);
    check receiver->ack(message);
    check receiver->close();
}

@test:Config {
    groups: ["smfReceiver", "smfValidation"]
}
isolated function testDirectReceiverWithoutSubscriptions() returns error? {
    DirectReceiver|Error receiver = new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        topicSubscriptions = []
    );
    if receiver !is Error {
        test:assertFail("Expected a validation error for a direct receiver without subscriptions");
    }
    test:assertTrue(receiver.message().includes("at least one topic subscription"));
}

@test:Config {
    groups: ["smfReceiver", "smfValidation"]
}
isolated function testPersistentReceiverConflictingAckModes() returns error? {
    PersistentReceiver|Error receiver = new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = RECEIVER_TEST_QUEUE,
        autoAck = true,
        negativeSettlementEnabled = true
    );
    if receiver !is Error {
        test:assertFail("Expected a validation error for autoAck combined with negative settlement");
    }
    test:assertTrue(receiver.message().includes("mutually exclusive"));
}
