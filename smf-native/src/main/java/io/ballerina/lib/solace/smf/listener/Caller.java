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

package io.ballerina.lib.solace.smf.listener;

import com.solace.messaging.config.MessageAcknowledgementConfiguration.Outcome;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.lib.solace.smf.receiver.MessageConverter;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

/**
 * Native class for the Ballerina Solace SMF Caller.
 */
public final class Caller {
    static final String NATIVE_RECEIVER = "native.smf.receiver";

    private Caller() {
    }

    public static Object ack(BObject caller, BMap<BString, Object> message) {
        return settle(caller, message, Outcome.ACCEPTED);
    }

    public static Object failed(BObject caller, BMap<BString, Object> message) {
        return settle(caller, message, Outcome.FAILED);
    }

    public static Object rejected(BObject caller, BMap<BString, Object> message) {
        return settle(caller, message, Outcome.REJECTED);
    }

    private static Object settle(BObject caller, BMap<BString, Object> message, Outcome outcome) {
        PersistentMessageReceiver receiver = (PersistentMessageReceiver) caller.getNativeData(NATIVE_RECEIVER);
        if (receiver == null) {
            return CommonUtils.createError(
                    String.format("Cannot settle message with outcome '%s': native receiver not found", outcome));
        }
        Object nativeMessage = message.getNativeData(MessageConverter.NATIVE_MESSAGE);
        if (nativeMessage == null) {
            return CommonUtils.createError(
                    String.format("Cannot settle message with outcome '%s': native message not found", outcome));
        }
        try {
            if (outcome == Outcome.ACCEPTED) {
                receiver.ack((InboundMessage) nativeMessage);
            } else {
                receiver.settle((InboundMessage) nativeMessage, outcome);
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while settling the message with outcome '%s': %s",
                            outcome, exception.getMessage()), exception);
        }
    }
}
