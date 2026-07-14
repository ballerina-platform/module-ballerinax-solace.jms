/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.org).
 *
 *  WSO2 LLC. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.ballerina.lib.solace.jms.consumer;

import io.ballerina.lib.solace.jms.CommonUtils;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * Utility methods for consumer operations.
 */
public final class ConsumerUtils {

    private ConsumerUtils() {}

    /**
     * Creates a JMS MessageConsumer based on subscription configuration.
     *
     * @param session            JMS session
     * @param subscriptionConfig subscription configuration
     * @return the created MessageConsumer, plus the resolved destination name (queue or topic)
     * @throws JMSException if consumer creation fails
     */
    public static ConsumerCreationResult createConsumer(Session session, SubscriptionConfig subscriptionConfig)
            throws JMSException {
        return switch (subscriptionConfig) {
            case QueueConfig queueConfig -> {
                Queue queue = CommonUtils.createQueue(session, queueConfig.queueName(), queueConfig.durability());
                MessageConsumer consumer = CommonUtils.createConsumerForQueue(
                        session, queue, queueConfig.messageSelector());
                yield new ConsumerCreationResult(consumer, queue.getQueueName());
            }
            case TopicConfig topicConfig -> {
                MessageConsumer consumer = CommonUtils.createTopicConsumer(
                        session,
                        topicConfig.topicName(),
                        topicConfig.messageSelector(),
                        topicConfig.durability(),
                        topicConfig.subscriberName()
                );
                yield new ConsumerCreationResult(consumer, topicConfig.topicName());
            }
        };
    }
}
