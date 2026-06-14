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

# Checks whether a given type is a subtype of `smf:Message`.
#
# + typeDesc - The type needed to be checked as a subtype of `smf:Message`
# + return - True if the type is a subtype of `smf:Message`, else false
isolated function isSmfMessage(typedesc<anydata> typeDesc) returns boolean {
    if typeDesc is typedesc<Message> {
        return true;
    }
    return false;
}

isolated function convertPayload(anydata payload) returns string|byte[] {
    if payload is string {
        return payload;
    } else if payload is byte[] {
        return payload;
    } else if payload is xml {
        return payload.toString();
    } else if payload is int|boolean|float|decimal {
        return payload.toString().toBytes();
    } else {
        return payload.toJsonString().toBytes();
    }
}

isolated function toInternalMessage(Message message) returns InternalMessage {
    return {
        payload: convertPayload(message.payload),
        correlationId: message.correlationId,
        properties: message.properties,
        applicationMessageId: message.applicationMessageId,
        applicationMessageType: message.applicationMessageType,
        priority: message.priority,
        timeToLive: message.timeToLive,
        dmqEligible: message.dmqEligible,
        sequenceNumber: message.sequenceNumber,
        senderId: message.senderId
    };
}
