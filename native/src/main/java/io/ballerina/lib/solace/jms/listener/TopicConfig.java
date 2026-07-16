/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.lib.solace.jms.listener;

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * Represents configuration details for consuming messages from a JMS topic subscription.
 *
 * @param ackMode The acknowledgement mode for message consumption. This determines how
 *                messages received by the session are acknowledged.
 *                Common values include "AUTO_ACKNOWLEDGE", "CLIENT_ACKNOWLEDGE", and "DUPS_OK_ACKNOWLEDGE".
 * @param topicName       The name of the JMS topic to subscribe to.
 *
 * @param messageSelector An optional JMS message selector expression. Only messages with properties
 *                        matching this selector will be delivered to the consumer.
 *                        If {@code null}, no message selector is applied.
 *
 * @param durability      The durability of the subscription. Expected values are "DURABLE" or "TEMPORARY".
 *
 * @param subscriberName  An optional name to identify the subscription, especially for durable
 *                        subscriptions.
 */
public record TopicConfig(String ackMode, String topicName, String messageSelector,
                          String durability, String subscriberName) implements ServiceConfig {
    private static final String DURABLE = "DURABLE";
    private static final BString ACK_MODE = StringUtils.fromString("ackMode");
    private static final BString TOPIC_NAME = StringUtils.fromString("topicName");
    private static final BString MSG_SELECTOR = StringUtils.fromString("messageSelector");
    private static final BString DURABILITY = StringUtils.fromString("durability");
    private static final BString SUBSCRIBER_NAME = StringUtils.fromString("subscriberName");

    public TopicConfig {
        if (DURABLE.equalsIgnoreCase(durability) && (subscriberName == null || subscriberName.isEmpty())) {
            throw new IllegalArgumentException("subscriberName is required when the topic is DURABLE");
        }
    }

    TopicConfig(BMap<BString, Object> configurations) {
        this(
                configurations.getStringValue(ACK_MODE).getValue(),
                configurations.getStringValue(TOPIC_NAME).getValue(),
                configurations.containsKey(MSG_SELECTOR) ?
                        configurations.getStringValue(MSG_SELECTOR).getValue() : null,
                configurations.getStringValue(DURABILITY).getValue(),
                configurations.containsKey(SUBSCRIBER_NAME) ?
                        configurations.getStringValue(SUBSCRIBER_NAME).getValue() : null
        );
    }
}
