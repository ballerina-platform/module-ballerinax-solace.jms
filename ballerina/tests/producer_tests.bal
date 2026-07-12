// Copyright (c) 2025 WSO2 LLC. (http://www.wso2.com).
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
import ballerina/time;

@test:Config {
    groups: ["producer"]
}
isolated function testProducerInitWithQueue() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testProducerInitWithTopic() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {topicName: TEST_TOPIC},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendTextMessage() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendBytesMessage() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    byte[] byteContent = TEXT_MESSAGE_CONTENT.toBytes();
    Message message = {
        payload: byteContent
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMapMessage() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    map<Value> mapContent = {
        "name": "John Doe",
        "age": 30,
        "active": true,
        "salary": 75000.50
    };
    Message message = {
        payload: mapContent
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithProperties() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        properties: {
            "priority": 5,
            "source": "ballerina-test",
            "urgent": true,
            "version": <byte>1,
            "ratio": <float>0.75
        }
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithCorrelationId() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        correlationId: "test-correlation-id-12345"
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithMessageType() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        messageType: "OrderMessage"
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithSenderId() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        senderId: "test-sender-123"
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendToTopic() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {topicName: TEST_TOPIC},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer", "transacted"]
}
isolated function testTransactedProducerCommit() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_TRANSACTED_QUEUE},
        messageVpn: MESSAGE_VPN,
        transacted: true,
        enableDynamicDurables: true,
        directTransport: false,
        directOptimized: false,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message1 = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message1);

    Message message2 = {
        payload: TEXT_MESSAGE_CONTENT_2
    };
    check producer->send(message2);

    check producer->'commit();
    check producer->close();
}

@test:Config {
    groups: ["producer", "transacted"]
}
isolated function testTransactedProducerRollback() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_TRANSACTED_QUEUE},
        messageVpn: MESSAGE_VPN,
        transacted: true,
        directTransport: false,
        directOptimized: false,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message);

    check producer->'rollback();
    check producer->close();
}

@test:Config {
    groups: ["producer", "config"]
}
isolated function testProducerWithCustomClientId() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        clientName: "test-client-123",
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    check producer->close();
}

@test:Config {
    groups: ["producer", "config"]
}
isolated function testProducerWithCompression() returns error? {
    MessageProducer producer = check new (BROKER_URL_COMPRESSED, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        compressionLevel: 5,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer", "config"]
}
isolated function testProducerWithRetryConfig() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        retryConfig: {
            connectRetries: 3,
            connectRetriesPerHost: 1,
            reconnectRetries: 5,
            reconnectRetryWait: 1.0
        }
    });
    check producer->close();
}

@test:Config {
    groups: ["producer", "negative"]
}
isolated function testProducerInitWithInvalidUrl() {
    MessageProducer|Error producer = new ("invalid-url", {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    test:assertTrue(producer is Error, "Expected error for invalid URL");
}

@test:Config {
    groups: ["producer", "negative"],
    enable: false
}
isolated function testProducerInitWithInvalidCredentials() {
    MessageProducer|Error producer = new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: "invalid-user",
            password: "invalid-password"
        }
    });
    test:assertTrue(producer is Error, "Expected error for invalid credentials");
}

@test:Config {
    groups: ["producer", "negative"]
}
isolated function testCommitWithoutTransaction() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        transacted: false,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Error? commitResult = producer->'commit();
    test:assertTrue(commitResult is Error, "Expected error when calling commit without transaction");
    check producer->close();
}

@test:Config {
    groups: ["producer", "negative"]
}
isolated function testRollbackWithoutTransaction() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        transacted: false,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Error? rollbackResult = producer->'rollback();
    test:assertTrue(rollbackResult is Error, "Expected error when calling rollback without transaction");
    check producer->close();
}

@test:Config {
    groups: ["producer", "validation"]
}
isolated function testProducerValidationWithInvalidCompressionLevel() {
    MessageProducer|Error producer = new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        compressionLevel: 15,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    test:assertTrue(producer is Error, "Expected validation error for compression level > 9");
    if producer is Error {
        test:assertTrue(producer.message().toLowerAscii().includes("compression"),
                "Error message should mention compression");
    }
}

@test:Config {
    groups: ["producer", "validation"]
}
isolated function testProducerValidationWithLongUsername() {
    MessageProducer|Error producer = new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        auth: {
            username: "this-is-a-very-long-username-that-exceeds-the-maximum-allowed-length",
            password: BROKER_PASSWORD
        }
    });
    test:assertTrue(producer is Error, "Expected validation error for username > 32 chars");
    if producer is Error {
        test:assertTrue(producer.message().toLowerAscii().includes("username"),
                "Error message should mention username");
    }
}

@test:Config {
    groups: ["producer", "validation"]
}
isolated function testProducerValidationTransactedRequiresGuaranteedDelivery() {
    MessageProducer|Error producer = new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        transacted: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });
    test:assertTrue(producer is Error,
            "Expected validation error when transacted is true with directTransport left at default true");
    if producer is Error {
        test:assertTrue(producer.message().toLowerAscii().includes("directtransport"),
                "Error message should mention directTransport");
    }
}

@test:Config {
    groups: ["producer", "xml"]
}
isolated function testSendXmlMessage() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: TEST_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    xml xmlPayload = xml `<order><id>12345</id><item>Widget</item><quantity>10</quantity></order>`;
    Message message = {
        payload: xmlPayload
    };
    check producer->send(message);
    check producer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithDeliveryModeAndPriority() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_DELIVERY_MODE_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        deliveryMode: NON_PERSISTENT,
        priority: 7
    };
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_DELIVERY_MODE_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message with delivery mode and priority set");
    if receivedMessage is Message {
        test:assertEquals(receivedMessage.deliveryMode, NON_PERSISTENT, "Delivery mode should be overridden to non-persistent");
        test:assertEquals(receivedMessage.priority, 7, "Priority should be overridden to 7");
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithTimeToLive() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_TTL_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    decimal ttlSeconds = 60d;
    decimal sendTime = <decimal>time:utcNow()[0] * 1000;
    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        timeToLive: ttlSeconds
    };
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_TTL_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message with time-to-live set");
    if receivedMessage is Message {
        int? expiration = receivedMessage?.expiration;
        test:assertTrue(expiration is int, "Expiration should be populated by the provider");
        if expiration is int {
            // Provider computes JMSExpiration = send time + timeToLive; allow a generous window
            // for clock skew/broker processing time rather than asserting an exact value.
            test:assertTrue(<decimal>expiration >= sendTime + (ttlSeconds * 1000d) - 5000d,
                    "Expiration should reflect the configured time-to-live");
        }
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithDefaultTimeToLive() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_DEFAULT_TTL_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_DEFAULT_TTL_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message with default (no) time-to-live");
    if receivedMessage is Message {
        // JMSExpiration is 0 (never expires) when no timeToLive was set on send.
        test:assertEquals(receivedMessage?.expiration, (), "Expiration should be unset when timeToLive is omitted");
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithDefaultDeliveryModeAndPriority() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_DEFAULT_DELIVERY_MODE_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT
    };
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_DEFAULT_DELIVERY_MODE_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message with default delivery mode and priority");
    if receivedMessage is Message {
        // Falls back to the producer's own defaults (see MessageConverter.resolveDeliveryMode/resolvePriority)
        // rather than any value hardcoded in this test.
        test:assertTrue(receivedMessage?.deliveryMode is DeliveryMode, "Delivery mode should be populated by the provider");
        test:assertTrue(receivedMessage?.priority is int, "Priority should be populated by the provider");
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithReplyToQueue() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_REPLY_TO_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        replyTo: {queueName: PRODUCER_REPLY_TO_DESTINATION_QUEUE}
    };
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_REPLY_TO_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message with replyTo queue set");
    if receivedMessage is Message {
        Destination? replyTo = receivedMessage.replyTo;
        test:assertTrue(replyTo is Queue, "replyTo should be resolved as a Queue");
        if replyTo is Queue {
            test:assertEquals(replyTo.queueName, PRODUCER_REPLY_TO_DESTINATION_QUEUE);
        }
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer"]
}
isolated function testSendMessageWithReplyToTopic() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_REPLY_TO_TOPIC_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {
        payload: TEXT_MESSAGE_CONTENT,
        replyTo: {topicName: PRODUCER_REPLY_TO_DESTINATION_TOPIC}
    };
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_REPLY_TO_TOPIC_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message with replyTo topic set");
    if receivedMessage is Message {
        Destination? replyTo = receivedMessage.replyTo;
        test:assertTrue(replyTo is Topic, "replyTo should be resolved as a Topic");
        if replyTo is Topic {
            test:assertEquals(replyTo.topicName, PRODUCER_REPLY_TO_DESTINATION_TOPIC);
        }
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer", "destination"]
}
isolated function testProducerSendUsesConfiguredDefaultDestination() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_DEFAULT_DESTINATION_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {payload: TEXT_MESSAGE_CONTENT};
    check producer->send(message);
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_DEFAULT_DESTINATION_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message sent to the configured default destination");
    if receivedMessage is Message {
        test:assertEquals(receivedMessage.payload, TEXT_MESSAGE_CONTENT);
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer", "destination"]
}
isolated function testProducerSendWithPerCallDestinationNoConfig() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {payload: TEXT_MESSAGE_CONTENT};
    check producer->send(message, {queueName: PRODUCER_NO_CONFIG_DESTINATION_QUEUE});
    check producer->close();

    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_NO_CONFIG_DESTINATION_QUEUE
        }
    });

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "Should receive message sent via a per-call destination " +
            "with no configured default");
    if receivedMessage is Message {
        test:assertEquals(receivedMessage.payload, TEXT_MESSAGE_CONTENT);
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer", "destination"]
}
isolated function testProducerSendPerCallDestinationOverridesConfigured() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_OVERRIDE_CONFIG_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {payload: TEXT_MESSAGE_CONTENT};
    check producer->send(message, {queueName: PRODUCER_OVERRIDE_TARGET_QUEUE});
    check producer->close();

    MessageConsumer targetConsumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_OVERRIDE_TARGET_QUEUE
        }
    });
    Message? receivedAtTarget = check targetConsumer->receive(5.0);
    test:assertTrue(receivedAtTarget is Message, "Should receive message at the per-call override destination");
    if receivedAtTarget is Message {
        test:assertEquals(receivedAtTarget.payload, TEXT_MESSAGE_CONTENT);
    }
    check targetConsumer->close();

    MessageConsumer configConsumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_OVERRIDE_CONFIG_QUEUE
        }
    });
    Message? receivedAtConfigured = check configConsumer->receive(1.0);
    test:assertTrue(receivedAtConfigured is (), "Configured default destination should not also receive " +
            "the message when a per-call destination overrides it");
    check configConsumer->close();
}

@test:Config {
    groups: ["producer", "destination"]
}
isolated function testProducerSendOverrideAcrossDestinationTypes() returns error? {
    // Topic subscriptions aren't durable by default, so the subscriber must exist before the
    // message is published - mirrors the testReceiveWithTopic idiom in consumer_tests.bal.
    MessageConsumer consumer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            topicName: PRODUCER_OVERRIDE_TARGET_TOPIC
        }
    });

    runtime:sleep(0.5);

    MessageProducer producer = check new (BROKER_URL, {
        destination: {queueName: PRODUCER_OVERRIDE_CONFIG_QUEUE},
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {payload: TEXT_MESSAGE_CONTENT};
    check producer->send(message, {topicName: PRODUCER_OVERRIDE_TARGET_TOPIC});
    check producer->close();

    Message? receivedMessage = check consumer->receive(5.0);
    test:assertTrue(receivedMessage is Message, "A topic override should work even though the configured " +
            "default destination is a queue");
    if receivedMessage is Message {
        test:assertEquals(receivedMessage.payload, TEXT_MESSAGE_CONTENT);
    }
    check consumer->close();
}

@test:Config {
    groups: ["producer", "destination"]
}
isolated function testProducerSendFanOutWithSameProducer() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    check producer->send({payload: TEXT_MESSAGE_CONTENT}, {queueName: PRODUCER_FANOUT_QUEUE_A});
    check producer->send({payload: TEXT_MESSAGE_CONTENT_2}, {queueName: PRODUCER_FANOUT_QUEUE_B});
    check producer->close();

    MessageConsumer consumerA = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_FANOUT_QUEUE_A
        }
    });
    Message? receivedA = check consumerA->receive(5.0);
    test:assertTrue(receivedA is Message, "Should receive the first fan-out message from the same producer");
    if receivedA is Message {
        test:assertEquals(receivedA.payload, TEXT_MESSAGE_CONTENT);
    }
    check consumerA->close();

    MessageConsumer consumerB = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        enableDynamicDurables: true,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        },
        subscriptionConfig: {
            queueName: PRODUCER_FANOUT_QUEUE_B
        }
    });
    Message? receivedB = check consumerB->receive(5.0);
    test:assertTrue(receivedB is Message, "Should receive the second fan-out message from the same producer");
    if receivedB is Message {
        test:assertEquals(receivedB.payload, TEXT_MESSAGE_CONTENT_2);
    }
    check consumerB->close();
}

@test:Config {
    groups: ["producer", "destination", "negative"]
}
isolated function testProducerSendWithNoDestinationReturnsError() returns error? {
    MessageProducer producer = check new (BROKER_URL, {
        messageVpn: MESSAGE_VPN,
        auth: {
            username: BROKER_USERNAME,
            password: BROKER_PASSWORD
        }
    });

    Message message = {payload: TEXT_MESSAGE_CONTENT};
    Error? result = producer->send(message);
    test:assertTrue(result is Error, "send() should fail when no destination is configured or provided");
    if result is Error {
        test:assertTrue(result.message().toLowerAscii().includes("destination"),
                "Error should mention the missing destination: " + result.message());
    }
    check producer->close();
}
