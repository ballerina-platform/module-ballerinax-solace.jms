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

package io.ballerina.lib.solace.jms.producer;

import io.ballerina.lib.solace.jms.DeliveryMode;
import io.ballerina.lib.solace.jms.config.ConfigUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Converter for Ballerina messages to JMS messages.
 */
public final class MessageConverter {

    private static final BString PAYLOAD = StringUtils.fromString("payload");
    private static final BString PROPERTIES = StringUtils.fromString("properties");
    private static final BString CORRELATION_ID = StringUtils.fromString("correlationId");
    private static final BString JMS_TYPE = StringUtils.fromString("jmsType");
    private static final BString REPLY_TO = StringUtils.fromString("replyTo");
    private static final BString DELIVERY_MODE = StringUtils.fromString("deliveryMode");
    private static final BString PRIORITY = StringUtils.fromString("priority");
    private static final BString TIME_TO_LIVE = StringUtils.fromString("timeToLive");

    private MessageConverter() {}

    /**
     * Converts Ballerina message to JMS message.
     *
     * @param session  JMS session
     * @param bMessage Ballerina message map
     * @return JMS message
     * @throws JMSException if message conversion fails
     */
    public static Message toJmsMessage(Session session, BMap<BString, Object> bMessage) throws JMSException {
        Object payload = bMessage.get(PAYLOAD);
        Message jmsMessage = createMessageByContentType(session, payload);

        // Set correlation ID if present
        if (bMessage.containsKey(CORRELATION_ID)) {
            Object correlationId = bMessage.get(CORRELATION_ID);
            if (correlationId instanceof BString bCorrelationId) {
                jmsMessage.setJMSCorrelationID(bCorrelationId.getValue());
            }
        }

        // Set JMS type if present
        if (bMessage.containsKey(JMS_TYPE)) {
            Object jmsType = bMessage.get(JMS_TYPE);
            if (jmsType instanceof BString bJmsType) {
                jmsMessage.setJMSType(bJmsType.getValue());
            }
        }

        // Set custom properties if present
        if (bMessage.containsKey(PROPERTIES)) {
            Object properties = bMessage.get(PROPERTIES);
            if (properties instanceof BMap<?, ?> propsMap) {
                setMessageProperties(jmsMessage, (BMap<BString, Object>) propsMap);
            }
        }

        // Set reply-to destination if present
        if (bMessage.containsKey(REPLY_TO)) {
            Object replyTo = bMessage.get(REPLY_TO);
            if (replyTo instanceof BMap<?, ?> replyToMap) {
                Destination replyToDestination =
                        ProducerConfiguration.getDestination((BMap<BString, Object>) replyToMap);
                jmsMessage.setJMSReplyTo(Actions.createDestination(session, replyToDestination));
            }
        }

        return jmsMessage;
    }

    /**
     * Resolves the delivery mode to use for a send operation.
     *
     * @param bMessage             Ballerina message map
     * @param defaultDeliveryMode  delivery mode to fall back to when not set on the message
     * @return the delivery mode set on the message, or {@code defaultDeliveryMode} if absent
     */
    static int resolveDeliveryMode(BMap<BString, Object> bMessage, int defaultDeliveryMode) {
        if (bMessage.containsKey(DELIVERY_MODE) && bMessage.get(DELIVERY_MODE) instanceof BString deliveryMode) {
            return DeliveryMode.fromString(deliveryMode.getValue());
        }
        return defaultDeliveryMode;
    }

    /**
     * Resolves the priority to use for a send operation.
     *
     * @param bMessage        Ballerina message map
     * @param defaultPriority priority to fall back to when not set on the message
     * @return the priority set on the message, or {@code defaultPriority} if absent
     */
    static int resolvePriority(BMap<BString, Object> bMessage, int defaultPriority) {
        if (bMessage.containsKey(PRIORITY) && bMessage.get(PRIORITY) instanceof Long priority) {
            return priority.intValue();
        }
        return defaultPriority;
    }

    /**
     * Resolves the time-to-live to use for a send operation.
     *
     * @param bMessage             Ballerina message map
     * @param defaultTimeToLive    time-to-live in milliseconds to fall back to when not set on the message
     * @return the time-to-live in milliseconds set on the message (converted from seconds), or
     *         {@code defaultTimeToLive} if absent
     */
    static long resolveTimeToLive(BMap<BString, Object> bMessage, long defaultTimeToLive) {
        if (bMessage.containsKey(TIME_TO_LIVE) && bMessage.get(TIME_TO_LIVE) instanceof BDecimal timeToLive) {
            return ConfigUtils.decimalToMillis(timeToLive.decimalValue());
        }
        return defaultTimeToLive;
    }

    private static Message createMessageByContentType(Session session, Object content) throws JMSException {
        if (content instanceof BString bString) {
            TextMessage message = session.createTextMessage();
            message.setText(bString.getValue());
            return message;
        } else if (content instanceof BArray bArray) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(bArray.getBytes());
            return message;
        } else if (content instanceof BMap<?, ?> bMap) {
            MapMessage message = session.createMapMessage();
            setMapMessageFields(message, (BMap<BString, Object>) bMap);
            return message;
        }
        throw new IllegalArgumentException("Unsupported message content type: "
                + (content != null ? content.getClass().getName() : "null"));
    }

    private static void setMapMessageFields(MapMessage message, BMap<BString, Object> map) throws JMSException {
        for (BString key : map.getKeys()) {
            Object value = map.get(key);
            String keyString = key.getValue();

            if (value instanceof BString bString) {
                message.setString(keyString, bString.getValue());
            } else if (value instanceof Long l) {
                message.setLong(keyString, l);
            } else if (value instanceof Integer i) {
                message.setInt(keyString, i);
            } else if (value instanceof Double d) {
                message.setDouble(keyString, d);
            } else if (value instanceof Boolean b) {
                message.setBoolean(keyString, b);
            } else if (value instanceof BArray bArray) {
                message.setBytes(keyString, bArray.getBytes());
            }
        }
    }

    private static void setMessageProperties(Message message, BMap<BString, Object> properties)
            throws JMSException {
        for (BString key : properties.getKeys()) {
            Object value = properties.get(key);
            String keyString = key.getValue();

            if (value instanceof BString bString) {
                message.setStringProperty(keyString, bString.getValue());
            } else if (value instanceof Long l) {
                message.setLongProperty(keyString, l);
            } else if (value instanceof Integer i) {
                message.setIntProperty(keyString, i);
            } else if (value instanceof Double d) {
                message.setDoubleProperty(keyString, d);
            } else if (value instanceof Boolean b) {
                message.setBooleanProperty(keyString, b);
            } else if (value instanceof Byte b) {
                message.setByteProperty(keyString, b);
            } else if (value instanceof Float f) {
                message.setFloatProperty(keyString, f);
            }
        }
    }
}
