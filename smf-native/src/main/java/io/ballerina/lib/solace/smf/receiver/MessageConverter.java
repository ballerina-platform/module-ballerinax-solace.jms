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

import com.solace.messaging.receiver.InboundMessage;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.ArrayType;
import io.ballerina.runtime.api.types.IntersectionType;
import io.ballerina.runtime.api.types.MapType;
import io.ballerina.runtime.api.types.PredefinedTypes;
import io.ballerina.runtime.api.types.RecordType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.JsonUtils;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.utils.ValueUtils;
import io.ballerina.runtime.api.utils.XmlUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Converter for Solace SMF inbound messages to Ballerina messages.
 */
public final class MessageConverter {

    public static final String NATIVE_MESSAGE = "native.smf.message";

    private static final BString PAYLOAD = StringUtils.fromString("payload");
    private static final BString CORRELATION_ID = StringUtils.fromString("correlationId");
    private static final BString PROPERTIES = StringUtils.fromString("properties");
    private static final BString APPLICATION_MESSAGE_ID = StringUtils.fromString("applicationMessageId");
    private static final BString APPLICATION_MESSAGE_TYPE = StringUtils.fromString("applicationMessageType");
    private static final BString PRIORITY = StringUtils.fromString("priority");
    private static final BString SEQUENCE_NUMBER = StringUtils.fromString("sequenceNumber");
    private static final BString SENDER_ID = StringUtils.fromString("senderId");
    private static final BString REDELIVERED = StringUtils.fromString("redelivered");
    private static final BString DESTINATION_NAME = StringUtils.fromString("destinationName");
    private static final BString REPLICATION_GROUP_MESSAGE_ID = StringUtils.fromString("replicationGroupMessageId");
    private static final BString EXPIRATION = StringUtils.fromString("expiration");
    private static final BString TIMESTAMP = StringUtils.fromString("timestamp");
    private static final BString SENDER_TIMESTAMP = StringUtils.fromString("senderTimestamp");

    // A boolean property of Solace messages denoting the text payload is an XML
    private static final String SOLACE_ISXML_PROPERTY = "JMS_Solace_isXML";

    private static final MapType STRING_MAP_TYPE = TypeCreator.createMapType(PredefinedTypes.TYPE_STRING);

    private MessageConverter() {
    }

    /**
     * Converts a Solace inbound message to the Ballerina {@code smf:Message} representation.
     *
     * @param inboundMessage received Solace message
     * @param expectedType   expected Ballerina record type
     * @return the Ballerina message record
     * @throws SmfDatabindingException if the payload cannot be bound to the expected type
     */
    public static BMap<BString, Object> toBallerinaMessage(InboundMessage inboundMessage, BTypedesc expectedType)
            throws SmfDatabindingException {
        RecordType recordType = getRecordType(expectedType.getDescribingType());
        BMap<BString, Object> ballerinaMessage = ValueCreator.createRecordValue(recordType);

        // Store the native message for later settlement
        ballerinaMessage.addNativeData(NATIVE_MESSAGE, inboundMessage);

        String correlationId = inboundMessage.getCorrelationId();
        if (correlationId != null) {
            ballerinaMessage.put(CORRELATION_ID, StringUtils.fromString(correlationId));
        }
        String applicationMessageId = inboundMessage.getApplicationMessageId();
        if (applicationMessageId != null) {
            ballerinaMessage.put(APPLICATION_MESSAGE_ID, StringUtils.fromString(applicationMessageId));
        }
        String applicationMessageType = inboundMessage.getApplicationMessageType();
        if (applicationMessageType != null) {
            ballerinaMessage.put(APPLICATION_MESSAGE_TYPE, StringUtils.fromString(applicationMessageType));
        }
        String senderId = inboundMessage.getSenderId();
        if (senderId != null) {
            ballerinaMessage.put(SENDER_ID, StringUtils.fromString(senderId));
        }
        String destinationName = inboundMessage.getDestinationName();
        if (destinationName != null) {
            ballerinaMessage.put(DESTINATION_NAME, StringUtils.fromString(destinationName));
        }
        InboundMessage.ReplicationGroupMessageId replicationGroupMessageId =
                inboundMessage.getReplicationGroupMessageId();
        if (replicationGroupMessageId != null) {
            ballerinaMessage.put(REPLICATION_GROUP_MESSAGE_ID,
                    StringUtils.fromString(replicationGroupMessageId.toString()));
        }

        ballerinaMessage.put(PRIORITY, (long) inboundMessage.getPriority());
        ballerinaMessage.put(REDELIVERED, inboundMessage.isRedelivered());

        long sequenceNumber = inboundMessage.getSequenceNumber();
        if (sequenceNumber > 0) {
            ballerinaMessage.put(SEQUENCE_NUMBER, sequenceNumber);
        }
        long expiration = inboundMessage.getExpiration();
        if (expiration > 0) {
            ballerinaMessage.put(EXPIRATION, expiration);
        }
        long timestamp = inboundMessage.getTimeStamp();
        if (timestamp > 0) {
            ballerinaMessage.put(TIMESTAMP, timestamp);
        }
        Long senderTimestamp = inboundMessage.getSenderTimestamp();
        if (senderTimestamp != null && senderTimestamp > 0) {
            ballerinaMessage.put(SENDER_TIMESTAMP, senderTimestamp);
        }

        Map<String, String> properties = inboundMessage.getProperties();
        if (properties != null && !properties.isEmpty()) {
            BMap<BString, Object> bProperties = ValueCreator.createMapValue(STRING_MAP_TYPE);
            properties.forEach((key, value) ->
                    bProperties.put(StringUtils.fromString(key), StringUtils.fromString(value)));
            ballerinaMessage.put(PROPERTIES, bProperties);
        }

        Type payloadType = TypeUtils.getReferredType(
                recordType.getFields().get(PAYLOAD.getValue()).getFieldType());
        ballerinaMessage.put(PAYLOAD, getPayloadWithIntendedType(inboundMessage, payloadType));

        return ballerinaMessage;
    }

    private static Object getPayloadWithIntendedType(InboundMessage message, Type payloadType)
            throws SmfDatabindingException {
        int typeTag = payloadType.getTag();
        try {
            switch (typeTag) {
                case TypeTags.STRING_TAG:
                    return StringUtils.fromString(textPayloadOrEmpty(message));
                case TypeTags.XML_TAG:
                    return XmlUtils.parse(textPayloadOrEmpty(message));
                case TypeTags.ANYDATA_TAG:
                    return getPayloadAsAnydata(message);
                case TypeTags.ARRAY_TAG:
                    Type elementType = TypeUtils.getReferredType(((ArrayType) payloadType).getElementType());
                    if (elementType.getTag() == TypeTags.BYTE_TAG) {
                        return ValueCreator.createArrayValue(bytesOrEmpty(message));
                    }
                    return getValueFromJson(payloadType, message);
                default:
                    return getValueFromJson(payloadType, message);
            }
        } catch (BError bError) {
            throw new SmfDatabindingException("Data binding failed: " + bError.getDetails());
        }
    }

    private static Object getPayloadAsAnydata(InboundMessage message) {
        if (message.hasProperty(SOLACE_ISXML_PROPERTY)
                && Boolean.parseBoolean(message.getProperty(SOLACE_ISXML_PROPERTY))) {
            return XmlUtils.parse(textPayloadOrEmpty(message));
        }
        String textPayload = message.getPayloadAsString();
        if (textPayload != null) {
            return StringUtils.fromString(textPayload);
        }
        byte[] bytes = message.getPayloadAsBytes();
        return ValueCreator.createArrayValue(bytes == null ? new byte[0] : bytes);
    }

    private static Object getValueFromJson(Type type, InboundMessage message) {
        byte[] bytes = bytesOrEmpty(message);
        String jsonString = new String(bytes, StandardCharsets.UTF_8);
        return ValueUtils.convert(JsonUtils.parse(jsonString), type);
    }

    // The payload accessors return null for an empty or no-body message (and getPayloadAsString also
    // returns null for a binary payload); these helpers provide the empty fallbacks the type-specific
    // conversions expect, so a payload-less message binds cleanly instead of raising a raw NPE.
    private static String textPayloadOrEmpty(InboundMessage message) {
        String textPayload = message.getPayloadAsString();
        return textPayload == null ? "" : textPayload;
    }

    private static byte[] bytesOrEmpty(InboundMessage message) {
        byte[] bytes = message.getPayloadAsBytes();
        return bytes == null ? new byte[0] : bytes;
    }

    private static RecordType getRecordType(Type type) {
        if (type.isReadOnly() && type instanceof IntersectionType intersectionType) {
            return (RecordType) TypeUtils.getReferredType(intersectionType.getConstituentTypes().get(0));
        }
        if (type.getTag() == TypeTags.INTERSECTION_TAG) {
            return (RecordType) TypeUtils.getReferredType(((IntersectionType) type).getConstituentTypes().get(0));
        }
        return (RecordType) TypeUtils.getReferredType(type);
    }
}
