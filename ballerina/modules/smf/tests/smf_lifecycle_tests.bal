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

// These queues and their topic mappings are provisioned on demand via CREATE_ON_START,
// keeping the lifecycle tests independent of init-solace.sh and of the other suites.
const string PAUSE_TEST_TOPIC = "smf/test/pause";
const string PAUSE_TEST_QUEUE = "smf-pause-queue";
const string DETACH_TEST_TOPIC = "smf/test/detach";
const string DETACH_TEST_QUEUE = "smf-detach-queue";
const string ONERROR_TEST_TOPIC = "smf/test/onerror";
const string ONERROR_TEST_QUEUE = "smf-onerror-queue";

isolated string? onErrorCaptured = ();

// Exercises the pause/resume flow control and, critically, the `'resume()` -> native
// `resumeReceiver` interop binding. After resume, delivery must continue normally.
@test:Config {
    groups: ["smfReceiver", "smfLifecycle"]
}
isolated function testPersistentReceiverPauseResume() returns error? {
    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = PAUSE_TEST_QUEUE,
        topicSubscriptions = [PAUSE_TEST_TOPIC],
        missingResourcesStrategy = CREATE_ON_START
    );
    check receiver->pause();
    check receiver->'resume();

    check publishPersistent("pause-resume test", PAUSE_TEST_TOPIC);
    Message? message = check receiver->receive(15.0);
    if message is () {
        check receiver->close();
        test:assertFail("Expected a message to be delivered after the receiver was resumed");
    }
    test:assertEquals(message.payload, "pause-resume test");
    check receiver->ack(message);
    check receiver->close();
}

// Detaching a service must terminate its receiver flow; a message published afterwards must
// remain spooled on the durable queue rather than being consumed by the detached service.
@test:Config {
    groups: ["smfService", "smfLifecycle"]
}
function testListenerDetach() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service queueService = @ServiceConfig {
        queueName: DETACH_TEST_QUEUE,
        topicSubscriptions: [DETACH_TEST_TOPIC],
        missingResourcesStrategy: CREATE_ON_START
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            check caller->ack(message);
        }
    };
    check smfListener.attach(queueService);
    check smfListener.'start();
    check smfListener.detach(queueService);

    // The subscription added during provisioning is durable, so the message is spooled even
    // though the service has been detached
    check publishPersistent("after detach", DETACH_TEST_TOPIC);

    PersistentReceiver verifier = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = DETACH_TEST_QUEUE
    );
    Message? leftover = check verifier->receive(10.0);
    check verifier->close();
    check smfListener.gracefulStop();
    if leftover is () {
        test:assertFail("Expected the post-detach message to remain on the queue");
    }
    test:assertEquals(leftover.payload, "after detach");
}

// When the onMessage handler returns an error, the dispatcher must route it to the service's
// onError remote method.
@test:Config {
    groups: ["smfService", "smfLifecycle"]
}
function testServiceOnErrorInvoked() returns error? {
    Listener smfListener = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Service faultyService = @ServiceConfig {
        queueName: ONERROR_TEST_QUEUE,
        topicSubscriptions: [ONERROR_TEST_TOPIC],
        missingResourcesStrategy: CREATE_ON_START
    } service object {
        remote function onMessage(Message message, Caller caller) returns error? {
            // Acknowledge first to avoid a redelivery storm, then fail to trigger onError
            check caller->ack(message);
            return error("intentional onMessage failure");
        }

        remote function onError(Error e) returns error? {
            lock {
                onErrorCaptured = e.message();
            }
        }
    };
    check smfListener.attach(faultyService);
    check smfListener.'start();

    check publishPersistent("trigger onError", ONERROR_TEST_TOPIC);

    string? captured = awaitPayload(isolated function() returns string? {
        lock {
            return onErrorCaptured;
        }
    });
    check smfListener.gracefulStop();
    if captured is () {
        test:assertFail("Expected the onError handler to be invoked when onMessage returns an error");
    }
    test:assertTrue((<string>captured).includes("intentional onMessage failure"),
        "Expected the onError handler to receive the error raised by onMessage");
}
