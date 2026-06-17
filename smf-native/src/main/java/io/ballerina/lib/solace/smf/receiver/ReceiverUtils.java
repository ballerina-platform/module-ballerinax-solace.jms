/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.org).
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

package io.ballerina.lib.solace.smf.receiver;

import com.solace.messaging.DirectMessageReceiverBuilder;
import com.solace.messaging.MessagingService;
import com.solace.messaging.PersistentMessageReceiverBuilder;
import com.solace.messaging.config.MessageAcknowledgementConfiguration.Outcome;
import com.solace.messaging.config.MissingResourcesCreationConfiguration.MissingResourcesCreationStrategy;
import com.solace.messaging.config.ReplayStrategy;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage.ReplicationGroupMessageId;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import com.solace.messaging.resources.Queue;
import com.solace.messaging.resources.ShareName;
import com.solace.messaging.resources.TopicSubscription;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

/**
 * Shared helpers for building SMF message receivers. Used by both the receiver client actions
 * and the listener; the receiver configuration records and the service configuration annotations
 * deliberately use the same field names.
 */
public final class ReceiverUtils {

    public static final String NATIVE_MESSAGING_SERVICE = "native.smf.messaging.service";
    public static final String NATIVE_RECEIVER = "native.smf.receiver";
    public static final long TERMINATE_GRACE_PERIOD_MILLIS = 10_000;

    private static final BString TOPIC_SUBSCRIPTIONS = StringUtils.fromString("topicSubscriptions");
    private static final BString SHARE_NAME = StringUtils.fromString("shareName");
    private static final BString QUEUE_NAME = StringUtils.fromString("queueName");
    private static final BString MESSAGE_SELECTOR = StringUtils.fromString("messageSelector");
    private static final BString MISSING_RESOURCES_STRATEGY = StringUtils.fromString("missingResourcesStrategy");
    private static final BString AUTO_ACK = StringUtils.fromString("autoAck");
    private static final BString NEGATIVE_SETTLEMENT_ENABLED = StringUtils.fromString("negativeSettlementEnabled");
    private static final BString REPLAY_STRATEGY = StringUtils.fromString("replayStrategy");
    private static final BString FROM_TIME = StringUtils.fromString("fromTime");
    private static final BString AFTER_MESSAGE_ID = StringUtils.fromString("afterMessageId");
    private static final String CREATE_ON_START = "CREATE_ON_START";

    private ReceiverUtils() {}

    /**
     * Builds and starts a direct message receiver from the Ballerina configuration.
     *
     * @param messagingService connected messaging service
     * @param config           Ballerina configuration map containing {@code topicSubscriptions}
     *                         and optionally {@code shareName}
     * @return the started receiver
     */
    public static DirectMessageReceiver buildDirectReceiver(MessagingService messagingService,
                                                            BMap<BString, Object> config) {
        DirectMessageReceiverBuilder builder = messagingService.createDirectMessageReceiverBuilder()
                .withSubscriptions(getTopicSubscriptions(config));
        if (config.containsKey(SHARE_NAME)) {
            return builder.build(ShareName.of(config.getStringValue(SHARE_NAME).getValue()));
        }
        return builder.build();
    }

    /**
     * Builds a persistent message receiver from the Ballerina configuration.
     *
     * @param messagingService connected messaging service
     * @param config           Ballerina configuration map containing {@code queueName} and the
     *                         optional subscription/settlement fields
     * @return the receiver (not started)
     */
    public static PersistentMessageReceiver buildPersistentReceiver(MessagingService messagingService,
                                                                    BMap<BString, Object> config) {
        PersistentMessageReceiverBuilder builder = messagingService.createPersistentMessageReceiverBuilder();

        TopicSubscription[] subscriptions = getTopicSubscriptions(config);
        if (subscriptions.length > 0) {
            builder.withSubscriptions(subscriptions);
        }
        if (config.containsKey(MESSAGE_SELECTOR)) {
            builder.withMessageSelector(config.getStringValue(MESSAGE_SELECTOR).getValue());
        }
        if (CREATE_ON_START.equals(config.getStringValue(MISSING_RESOURCES_STRATEGY).getValue())) {
            builder.withMissingResourcesCreationStrategy(MissingResourcesCreationStrategy.CREATE_ON_START);
        }
        if (config.getBooleanValue(AUTO_ACK)) {
            builder.withMessageAutoAcknowledgement();
        } else {
            builder.withMessageClientAcknowledgement();
        }
        if (config.getBooleanValue(NEGATIVE_SETTLEMENT_ENABLED)) {
            builder.withRequiredMessageClientOutcomeOperationSupport(Outcome.FAILED, Outcome.REJECTED);
        }
        if (config.containsKey(REPLAY_STRATEGY)) {
            builder.withMessageReplay(getReplayStrategy(config.get(REPLAY_STRATEGY)));
        }
        return builder.build(Queue.durableExclusiveQueue(config.getStringValue(QUEUE_NAME).getValue()));
    }

    private static ReplayStrategy getReplayStrategy(Object replayStrategy) {
        if (replayStrategy instanceof BString) {
            return ReplayStrategy.allMessages();
        }
        BMap<BString, Object> strategyConfig = (BMap<BString, Object>) replayStrategy;
        if (strategyConfig.containsKey(AFTER_MESSAGE_ID)) {
            return ReplayStrategy.replicationGroupMessageIdBased(
                    ReplicationGroupMessageId.of(strategyConfig.getStringValue(AFTER_MESSAGE_ID).getValue()));
        }
        // time:Utc is a tuple of [epoch seconds, fraction of a second]; preserve the fractional
        // component as a nano adjustment instead of truncating to whole seconds.
        BArray utcTime = strategyConfig.getArrayValue(FROM_TIME);
        long epochSeconds = utcTime.getInt(0);
        long nanoAdjustment = ((BDecimal) utcTime.get(1)).decimalValue()
                .multiply(BigDecimal.valueOf(1_000_000_000L)).longValue();
        Instant fromInstant = Instant.ofEpochSecond(epochSeconds, nanoAdjustment);
        return ReplayStrategy.timeBased(ZonedDateTime.ofInstant(fromInstant, ZoneOffset.UTC));
    }

    public static boolean isQueueSubscription(BMap<BString, Object> config) {
        return config.containsKey(QUEUE_NAME);
    }

    public static boolean isAutoAck(BMap<BString, Object> config) {
        return config.containsKey(AUTO_ACK) && config.getBooleanValue(AUTO_ACK);
    }

    public static boolean isNegativeSettlementEnabled(BMap<BString, Object> config) {
        return config.containsKey(NEGATIVE_SETTLEMENT_ENABLED)
                && config.getBooleanValue(NEGATIVE_SETTLEMENT_ENABLED);
    }

    private static TopicSubscription[] getTopicSubscriptions(BMap<BString, Object> config) {
        BArray topicSubscriptions = config.getArrayValue(TOPIC_SUBSCRIPTIONS);
        if (topicSubscriptions == null) {
            return new TopicSubscription[0];
        }
        return Arrays.stream(topicSubscriptions.getStringArray())
                .map(TopicSubscription::of)
                .toArray(TopicSubscription[]::new);
    }
}
