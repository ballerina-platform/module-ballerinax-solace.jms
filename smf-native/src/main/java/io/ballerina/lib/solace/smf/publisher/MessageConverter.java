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

package io.ballerina.lib.solace.smf.publisher;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.SolaceProperties.MessageProperties;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.OutboundMessageBuilder;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;
import java.util.Properties;

/**
 * Converts a Ballerina {@code smf:Message} representation into a Solace {@link OutboundMessage}.
 */
public final class MessageConverter {

    private static final BString PAYLOAD = StringUtils.fromString("payload");
    private static final BString CORRELATION_ID = StringUtils.fromString("correlationId");
    private static final BString PROPERTIES = StringUtils.fromString("properties");
    private static final BString APPLICATION_MESSAGE_ID = StringUtils.fromString("applicationMessageId");
    private static final BString APPLICATION_MESSAGE_TYPE = StringUtils.fromString("applicationMessageType");
    private static final BString PRIORITY = StringUtils.fromString("priority");
    private static final BString TIME_TO_LIVE = StringUtils.fromString("timeToLive");
    private static final BString DMQ_ELIGIBLE = StringUtils.fromString("dmqEligible");
    private static final BString SEQUENCE_NUMBER = StringUtils.fromString("sequenceNumber");
    private static final BString SENDER_ID = StringUtils.fromString("senderId");

    private MessageConverter() {}

    /**
     * Builds a Solace {@link OutboundMessage} from the Ballerina message representation.
     *
     * @param service  the messaging service used to create the message builder
     * @param bMessage Ballerina internal message map with a {@code string} or {@code byte[]} payload
     * @return the outbound message
     */
    public static OutboundMessage toOutboundMessage(MessagingService service, BMap<BString, Object> bMessage) {
        OutboundMessageBuilder builder = service.messageBuilder();

        if (bMessage.containsKey(APPLICATION_MESSAGE_ID)) {
            builder.withApplicationMessageId(bMessage.getStringValue(APPLICATION_MESSAGE_ID).getValue());
        }
        if (bMessage.containsKey(APPLICATION_MESSAGE_TYPE)) {
            builder.withApplicationMessageType(bMessage.getStringValue(APPLICATION_MESSAGE_TYPE).getValue());
        }
        if (bMessage.containsKey(PRIORITY)) {
            builder.withPriority(bMessage.getIntValue(PRIORITY).intValue());
        }
        if (bMessage.containsKey(TIME_TO_LIVE)) {
            builder.withTimeToLive(decimalToMillis(((BDecimal) bMessage.get(TIME_TO_LIVE)).decimalValue()));
        }
        if (bMessage.containsKey(SEQUENCE_NUMBER)) {
            builder.withSequenceNumber(bMessage.getIntValue(SEQUENCE_NUMBER));
        }
        if (bMessage.containsKey(SENDER_ID)) {
            builder.withSenderId(bMessage.getStringValue(SENDER_ID).getValue());
        }

        if (bMessage.containsKey(PROPERTIES)) {
            BMap<BString, Object> userProperties = (BMap<BString, Object>) bMessage.getMapValue(PROPERTIES);
            for (BString key : userProperties.getKeys()) {
                builder.withProperty(key.getValue(), userProperties.getStringValue(key).getValue());
            }
        }

        // Message properties that have no dedicated builder method are set via additional properties
        Properties additionalProperties = new Properties();
        if (bMessage.containsKey(CORRELATION_ID)) {
            additionalProperties.setProperty(MessageProperties.CORRELATION_ID,
                    bMessage.getStringValue(CORRELATION_ID).getValue());
        }
        additionalProperties.setProperty(MessageProperties.PERSISTENT_DMQ_ELIGIBLE,
                Boolean.toString(bMessage.getBooleanValue(DMQ_ELIGIBLE)));

        Object payload = bMessage.get(PAYLOAD);
        if (payload instanceof BString textPayload) {
            return builder.build(textPayload.getValue(), additionalProperties);
        }
        return builder.build(((BArray) payload).getBytes(), additionalProperties);
    }

    private static long decimalToMillis(BigDecimal seconds) {
        return seconds.multiply(BigDecimal.valueOf(1000)).longValue();
    }
}
