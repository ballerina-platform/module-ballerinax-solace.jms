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

import ballerina/lang.runtime;
import ballerina/test;

isolated int duplicateAttachReceivedCount = 0;

// A duplicate `attach()` of the *same* service instance must be rejected, and the originally attached
// service must keep working undisturbed. Mirrors the reproducer in
// https://github.com/wso2/product-integrator/issues/2015.
@test:Config {
    groups: ["listener"]
}
isolated function testListenerRejectsDuplicateAttach() returns error? {
    Listener freshListener = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });

    Service duplicateAttachService = @ServiceConfig {
        queueName: DUPLICATE_ATTACH_QUEUE
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                duplicateAttachReceivedCount += 1;
            }
        }
    };

    check freshListener.attach(duplicateAttachService);
    Error? duplicate = freshListener.attach(duplicateAttachService);
    test:assertTrue(duplicate is Error, "Attaching the same service instance twice must be rejected");

    check freshListener.'start();

    final MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: DUPLICATE_ATTACH_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });
    check producer->send({payload: "duplicate-attach-payload".toBytes()});
    check producer->close();
    runtime:sleep(2);
    check freshListener.gracefulStop();

    lock {
        test:assertEquals(duplicateAttachReceivedCount, 1,
                "Exactly one message should be delivered to the originally attached service");
    }
}

isolated int duplicateAttachSvc1Count = 0;
isolated int duplicateAttachSvc2Count = 0;

// Attaching two *different* service instances to the same listener must always be allowed - the guard is
// about rejecting the same object twice, not about limiting a listener to one service.
@test:Config {
    groups: ["listener"]
}
isolated function testListenerAllowsTwoDifferentServices() returns error? {
    Listener freshListener = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });

    Service duplicateAttachSvc1 = @ServiceConfig {
        queueName: DUPLICATE_ATTACH_SVC1_QUEUE
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                duplicateAttachSvc1Count += 1;
            }
        }
    };
    Service duplicateAttachSvc2 = @ServiceConfig {
        queueName: DUPLICATE_ATTACH_SVC2_QUEUE
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                duplicateAttachSvc2Count += 1;
            }
        }
    };

    check freshListener.attach(duplicateAttachSvc1);
    check freshListener.attach(duplicateAttachSvc2);
    check freshListener.'start();

    final MessageProducer producer1 = check new (BROKER_URL, {
        destination: {queueName: DUPLICATE_ATTACH_SVC1_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });
    final MessageProducer producer2 = check new (BROKER_URL, {
        destination: {queueName: DUPLICATE_ATTACH_SVC2_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });
    check producer1->send({payload: "svc1-payload".toBytes()});
    check producer2->send({payload: "svc2-payload".toBytes()});
    check producer1->close();
    check producer2->close();
    runtime:sleep(2);
    check freshListener.gracefulStop();

    lock {
        test:assertEquals(duplicateAttachSvc1Count, 1, "Service 1 should have received its own message");
    }
    lock {
        test:assertEquals(duplicateAttachSvc2Count, 1, "Service 2 should have received its own message");
    }
}

isolated int duplicateAttachReattachCount = 0;

// detach() followed by a fresh attach() of the same service instance must succeed - the duplicate-attach
// guard must not confuse "already attached" with "was attached once, then properly detached".
@test:Config {
    groups: ["listener"]
}
isolated function testListenerAllowsReattachAfterDetach() returns error? {
    Listener freshListener = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });

    Service duplicateAttachReattachService = @ServiceConfig {
        queueName: DUPLICATE_ATTACH_REATTACH_QUEUE
    } service object {
        remote function onMessage(Message message) returns error? {
            lock {
                duplicateAttachReattachCount += 1;
            }
        }
    };

    check freshListener.attach(duplicateAttachReattachService);
    check freshListener.detach(duplicateAttachReattachService);
    check freshListener.attach(duplicateAttachReattachService);
    check freshListener.'start();

    final MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: DUPLICATE_ATTACH_REATTACH_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {username: BROKER_USERNAME, password: BROKER_PASSWORD}
    });
    check producer->send({payload: "reattach-payload".toBytes()});
    check producer->close();
    runtime:sleep(2);
    check freshListener.gracefulStop();

    lock {
        test:assertEquals(duplicateAttachReattachCount, 1,
                "The re-attached service should receive the message after being detached and re-attached");
    }
}
