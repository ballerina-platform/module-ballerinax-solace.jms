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

import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

/**
 * Topic subscription configuration.
 *
 * @param ackMode Session acknowledgement mode
 * @param topicName Topic name
 * @param messageSelector Message selector (optional)
 * @param durability DURABLE or TEMPORARY
 * @param subscriberName Subscriber name for durable subscriptions
 */
public record TopicConfig(
        AcknowledgementMode ackMode,
        String topicName,
        String messageSelector,
        Durability durability,
        String subscriberName) implements SubscriptionConfig {

    private static final BString ACK_MODE_KEY = StringUtils.fromString("ackMode");
    private static final BString TOPIC_NAME_KEY = StringUtils.fromString("topicName");
    private static final BString MESSAGE_SELECTOR_KEY = StringUtils.fromString("messageSelector");
    private static final BString DURABILITY_KEY = StringUtils.fromString("durability");
    private static final BString SUBSCRIBER_NAME_KEY = StringUtils.fromString("subscriberName");

    public TopicConfig(BMap<BString, Object> config) {
        this(
                AcknowledgementMode.valueOf(
                        config.getStringValue(ACK_MODE_KEY).getValue()),
                config.getStringValue(TOPIC_NAME_KEY).getValue(),
                config.containsKey(MESSAGE_SELECTOR_KEY)
                        ? config.getStringValue(MESSAGE_SELECTOR_KEY).getValue()
                        : null,
                Durability.valueOf(
                        config.getStringValue(DURABILITY_KEY).getValue()),
                config.containsKey(SUBSCRIBER_NAME_KEY)
                        ? config.getStringValue(SUBSCRIBER_NAME_KEY).getValue()
                        : null
        );
    }
}
