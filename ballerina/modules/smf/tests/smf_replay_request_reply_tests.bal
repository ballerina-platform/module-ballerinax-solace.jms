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
import ballerina/time;

// The replay log `test-replay-log` and this queue are provisioned by init-solace.sh.
// The replay log must exist before the test messages are published.
const string REPLAY_TEST_TOPIC = "smf/test/replay";
const string REPLAY_TEST_QUEUE = "smf-replay-queue";
const string ECHO_TOPIC = "smf/test/echo";

isolated string? firstReplayedMessageId = ();
// Captured after the replay log exists but before the test messages are published; a replay
// request with a start time before the replay log's creation time is rejected by the broker
isolated time:Utc? replayStartTime = ();
// The VPN-wide replay log persists across test runs, so the replayed stream can contain
// messages from earlier runs. A per-run nonce identifies this run's messages.
final string replayNonce = time:utcNow()[0].toString();

# Receives and acknowledges all messages currently delivered to the receiver and returns
# their payloads in order.
isolated function receiveAll(PersistentReceiver receiver, decimal firstTimeout = 15.0) returns string[]|error {
    string[] payloads = [];
    do {
        decimal timeout = firstTimeout;
        while true {
            Message? message = check receiver->receive(timeout);
            if message is () {
                break;
            }
            anydata payload = message.payload;
            payloads.push(payload is string ? payload : payload.toString());
            check receiver->ack(message);
            timeout = 3.0;
        }
    } on fail error e {
        // The caller closes the receiver on the success path; on failure `check` would short-circuit
        // past that close, so release the broker flow/connection here before propagating the error.
        error? closeResult = receiver->close();
        return e;
    }
    return payloads;
}

@test:Config {
    groups: ["smfReplay"]
}
isolated function testAllMessagesReplay() returns error? {
    check drainQueue(REPLAY_TEST_QUEUE);
    // Back off a few seconds to tolerate clock skew between the test host and the broker;
    // the resulting time must still not precede the replay log's creation time
    lock {
        replayStartTime = time:utcAddSeconds(time:utcNow(), -5.0);
    }
    string firstPayload = string `replay message 1 ${replayNonce}`;
    string secondPayload = string `replay message 2 ${replayNonce}`;

    // Publish two messages and consume them normally
    check publishPersistent(firstPayload, REPLAY_TEST_TOPIC);
    check publishPersistent(secondPayload, REPLAY_TEST_TOPIC);

    PersistentReceiver receiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = REPLAY_TEST_QUEUE
    );
    string[] consumed = check receiveAll(receiver);
    check receiver->close();
    test:assertEquals(consumed, [firstPayload, secondPayload]);

    // A new receiver with the all-messages replay strategy must receive the already-consumed
    // messages again from the replay log. The log may also contain messages from earlier runs.
    PersistentReceiver replayReceiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = REPLAY_TEST_QUEUE,
        replayStrategy = ALL_MESSAGES
    );
    string? capturedMessageId = ();
    string[] replayed = [];
    decimal timeout = 15.0;
    while true {
        Message? message = check replayReceiver->receive(timeout);
        if message is () {
            break;
        }
        anydata payload = message.payload;
        string payloadText = payload is string ? payload : payload.toString();
        replayed.push(payloadText);
        if payloadText == firstPayload {
            capturedMessageId = message.replicationGroupMessageId;
        }
        check replayReceiver->ack(message);
        timeout = 3.0;
    }
    check replayReceiver->close();

    test:assertTrue(replayed.indexOf(firstPayload) !is (),
        "Expected the first message of this run to be replayed");
    test:assertTrue(replayed.indexOf(secondPayload) !is (),
        "Expected the second message of this run to be replayed");
    test:assertTrue(capturedMessageId is string,
        "Expected a replication group message id on the replayed message");
    lock {
        firstReplayedMessageId = capturedMessageId;
    }
}

@test:Config {
    groups: ["smfReplay"],
    dependsOn: [testAllMessagesReplay]
}
isolated function testReplicationGroupIdReplay() returns error? {
    string? afterMessageId;
    lock {
        afterMessageId = firstReplayedMessageId;
    }
    if afterMessageId is () {
        test:assertFail("Expected the replication group message id from the previous replay test");
    }

    // Replaying after this run's first message must deliver this run's second message,
    // but not the first one
    PersistentReceiver replayReceiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = REPLAY_TEST_QUEUE,
        replayStrategy = {afterMessageId}
    );
    string[] replayed = check receiveAll(replayReceiver);
    check replayReceiver->close();

    test:assertTrue(replayed.indexOf(string `replay message 2 ${replayNonce}`) !is (),
        "Expected the second message of this run to be replayed");
    test:assertTrue(replayed.indexOf(string `replay message 1 ${replayNonce}`) is (),
        "The message used as the replay starting point must not be replayed");
}

@test:Config {
    groups: ["smfReplay"],
    dependsOn: [testReplicationGroupIdReplay]
}
isolated function testTimeBasedReplay() returns error? {
    // Replay from just before this run's messages were published; the start time must not
    // precede the replay log's creation time. Earlier runs' messages are excluded by time.
    time:Utc? fromTime;
    lock {
        fromTime = replayStartTime;
    }
    if fromTime is () {
        test:assertFail("Expected the replay start time from the all-messages replay test");
    }
    PersistentReceiver replayReceiver = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        queueName = REPLAY_TEST_QUEUE,
        replayStrategy = {fromTime}
    );
    string[] replayed = check receiveAll(replayReceiver);
    check replayReceiver->close();

    test:assertEquals(replayed,
        [string `replay message 1 ${replayNonce}`, string `replay message 2 ${replayNonce}`],
        "Expected exactly this run's messages to be replayed from the given time");
}

isolated boolean stopReplier = false;

// Serves echo requests in a loop until signalled to stop. The request leg of request-reply uses
// direct (at-most-once) messaging, so a request may be dropped while the flow is being set up;
// looping lets the replier answer whichever retried request arrives.
isolated function serveEchoRequests(Replier replier) returns error? {
    while true {
        boolean stop;
        lock {
            stop = stopReplier;
        }
        if stop {
            break;
        }
        Message? request = check replier->receive(2.0);
        if request is () {
            continue;
        }
        anydata payload = request.payload;
        string requestText = payload is string ? payload : payload.toString();
        check replier->reply(request, string `echo: ${requestText}`);
    }
    check replier->close();
}

@test:Config {
    groups: ["smfRequestReply"]
}
function testRequestReply() returns error? {
    // Create the replier synchronously first so its subscription is established, then serve in a
    // loop. Retry the request because its direct (at-most-once) leg may be dropped under load.
    Replier replier = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD},
        topicSubscription = ECHO_TOPIC
    );
    future<error?> replierJob = start serveEchoRequests(replier);

    Requester requester = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Message? reply = ();
    foreach int _ in 0 ..< 5 {
        Message|Error attempt = requester->request("ping", ECHO_TOPIC, replyTimeout = 5.0);
        if attempt is Message {
            reply = attempt;
            break;
        }
    }
    check requester->close();
    lock {
        stopReplier = true;
    }

    error? replierResult = wait replierJob;
    if replierResult is error {
        test:assertFail(string `Replier failed: ${replierResult.message()}`);
    }
    if reply is () {
        test:assertFail("Did not receive a reply after retrying the request");
    }
    test:assertEquals(reply.payload, "echo: ping");
}

@test:Config {
    groups: ["smfRequestReply"]
}
isolated function testRequestWithoutReplier() returns error? {
    Requester requester = check new (BROKER_URL,
        auth = {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    );
    Message|Error reply = requester->request("unanswered", "smf/test/no-replier", replyTimeout = 3.0);
    check requester->close();
    if reply !is Error {
        test:assertFail("Expected an error when no replier is subscribed to the request topic");
    }
}
