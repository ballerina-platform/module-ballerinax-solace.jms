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
 * Queue subscription configuration.
 *
 * @param ackMode Session acknowledgement mode
 * @param queueName Queue name - required unless durability is TEMPORARY. Cannot be specified when durability is
 *                  TEMPORARY (real JMS temporary queues are always provider-named, {@code createTemporaryQueue()}
 *                  takes no name argument)
 * @param messageSelector Message selector (optional)
 * @param durability DURABLE or TEMPORARY
 */
public record QueueConfig(
        AcknowledgementMode ackMode,
        String queueName,
        String messageSelector,
        Durability durability) implements SubscriptionConfig {

    private static final BString ACK_MODE_KEY = StringUtils.fromString("ackMode");
    private static final BString QUEUE_NAME_KEY = StringUtils.fromString("queueName");
    private static final BString MESSAGE_SELECTOR_KEY = StringUtils.fromString("messageSelector");
    private static final BString DURABILITY_KEY = StringUtils.fromString("durability");

    public QueueConfig(BMap<BString, Object> config) {
        this(
                AcknowledgementMode.valueOf(
                        config.getStringValue(ACK_MODE_KEY).getValue()),
                config.containsKey(QUEUE_NAME_KEY)
                        ? config.getStringValue(QUEUE_NAME_KEY).getValue()
                        : null,
                config.containsKey(MESSAGE_SELECTOR_KEY)
                        ? config.getStringValue(MESSAGE_SELECTOR_KEY).getValue()
                        : null,
                Durability.valueOf(config.getStringValue(DURABILITY_KEY).getValue())
        );
        validate();
    }

    public boolean isTemporary() {
        return durability == Durability.TEMPORARY;
    }

    private void validate() {
        boolean hasQueueName = queueName != null && !queueName.isEmpty();
        if (!isTemporary() && !hasQueueName) {
            throw new IllegalArgumentException("queueName is required when durability is not TEMPORARY");
        }
        if (isTemporary() && hasQueueName) {
            throw new IllegalArgumentException("queueName cannot be specified when durability is TEMPORARY");
        }
    }
}
