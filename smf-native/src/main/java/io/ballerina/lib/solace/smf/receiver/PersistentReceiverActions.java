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

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.MessageAcknowledgementConfiguration.Outcome;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.ballerina.lib.solace.smf.receiver.ReceiverUtils.NATIVE_MESSAGING_SERVICE;
import static io.ballerina.lib.solace.smf.receiver.ReceiverUtils.NATIVE_RECEIVER;
import static io.ballerina.lib.solace.smf.receiver.ReceiverUtils.TERMINATE_GRACE_PERIOD_MILLIS;

/**
 * Actions class for {@link PersistentMessageReceiver} with utility methods to invoke as inter-op functions.
 */
public final class PersistentReceiverActions {

    // Margin added to the receive timeout when waiting on the result future, to absorb virtual-thread
    // scheduling overhead so a normally-completing receive is never cut off by the outer wait.
    private static final long GET_TIMEOUT_MARGIN_MILLIS = 10_000;

    private PersistentReceiverActions() {}

    /**
     * Creates a {@link PersistentMessageReceiver} using the broker URL and receiver configurations.
     *
     * @param receiver Ballerina receiver object
     * @param url      Solace broker URL
     * @param config   Ballerina receiver configurations
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object init(BObject receiver, BString url, BMap<BString, Object> config) {
        MessagingService messagingService = null;
        try {
            messagingService = CommonUtils.connect(url.getValue(), config);
            PersistentMessageReceiver persistentReceiver =
                    ReceiverUtils.buildPersistentReceiver(messagingService, config);
            persistentReceiver.start();
            receiver.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            receiver.addNativeData(NATIVE_RECEIVER, persistentReceiver);
            return null;
        } catch (Exception exception) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(
                    String.format("Error occurred while initializing the Solace persistent message receiver: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Receives the next message from the queue within the specified timeout.
     *
     * @param receiver  Ballerina receiver object
     * @param timeout   timeout in seconds
     * @param bTypedesc expected message type
     * @return Ballerina message, {@code null} if no message available, or {@code smf:Error} on failure
     */
    public static Object receive(BObject receiver, BDecimal timeout, BTypedesc bTypedesc) {
        PersistentMessageReceiver persistentReceiver =
                (PersistentMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
        long timeoutMillis = timeout.decimalValue().multiply(BigDecimal.valueOf(1000)).longValue();

        CompletableFuture<Object> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                InboundMessage message = persistentReceiver.receiveMessage(timeoutMillis);
                if (message == null) {
                    future.complete(null);
                } else {
                    future.complete(MessageConverter.toBallerinaMessage(message, bTypedesc));
                }
            } catch (SmfDatabindingException exception) {
                future.complete(CommonUtils.createError(exception.getMessage(), exception));
            } catch (Exception exception) {
                future.complete(CommonUtils.createError(
                        String.format("Error occurred while receiving message from Solace broker: %s",
                                exception.getMessage()), exception));
            }
        });

        try {
            // Bound the wait so a stalled receive cannot block the caller indefinitely; the receive
            // call self-limits to timeoutMillis, so the margin only guards against an unexpected stall.
            return future.get(timeoutMillis + GET_TIMEOUT_MARGIN_MILLIS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException exception) {
            return CommonUtils.createError(
                    String.format("Receive operation did not complete within %d ms",
                            timeoutMillis + GET_TIMEOUT_MARGIN_MILLIS), exception);
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while waiting for operation to complete: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Settles the message with the ACCEPTED outcome.
     *
     * @param receiver Ballerina receiver object
     * @param message  Ballerina message to acknowledge
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object ack(BObject receiver, BMap<BString, Object> message) {
        return settle(receiver, message, Outcome.ACCEPTED);
    }

    /**
     * Settles the message with the FAILED outcome.
     *
     * @param receiver Ballerina receiver object
     * @param message  Ballerina message to settle as failed
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object failed(BObject receiver, BMap<BString, Object> message) {
        return settle(receiver, message, Outcome.FAILED);
    }

    /**
     * Settles the message with the REJECTED outcome.
     *
     * @param receiver Ballerina receiver object
     * @param message  Ballerina message to settle as rejected
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object rejected(BObject receiver, BMap<BString, Object> message) {
        return settle(receiver, message, Outcome.REJECTED);
    }

    private static Object settle(BObject receiver, BMap<BString, Object> message, Outcome outcome) {
        PersistentMessageReceiver persistentReceiver =
                (PersistentMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
        Object nativeMessage = message.getNativeData(MessageConverter.NATIVE_MESSAGE);
        if (nativeMessage == null) {
            return CommonUtils.createError(
                    String.format("Cannot settle message with outcome '%s': native message not found", outcome));
        }
        try {
            if (outcome == Outcome.ACCEPTED) {
                persistentReceiver.ack((InboundMessage) nativeMessage);
            } else {
                persistentReceiver.settle((InboundMessage) nativeMessage, outcome);
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while settling the message with outcome '%s': %s",
                            outcome, exception.getMessage()), exception);
        }
    }

    /**
     * Pauses message delivery to the receiver.
     *
     * @param receiver Ballerina receiver object
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object pause(BObject receiver) {
        PersistentMessageReceiver persistentReceiver =
                (PersistentMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
        try {
            persistentReceiver.pause();
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while pausing the message receiver: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Resumes message delivery to the receiver.
     *
     * @param receiver Ballerina receiver object
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object resumeReceiver(BObject receiver) {
        PersistentMessageReceiver persistentReceiver =
                (PersistentMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
        try {
            persistentReceiver.resume();
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while resuming the message receiver: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Terminates the receiver and disconnects the underlying messaging service.
     *
     * @param receiver Ballerina receiver object
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object close(BObject receiver) {
        try {
            PersistentMessageReceiver persistentReceiver =
                    (PersistentMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
            MessagingService messagingService = (MessagingService) receiver.getNativeData(NATIVE_MESSAGING_SERVICE);
            if (persistentReceiver != null) {
                persistentReceiver.terminate(TERMINATE_GRACE_PERIOD_MILLIS);
            }
            if (messagingService != null) {
                messagingService.disconnect();
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while closing the persistent message receiver: %s",
                            exception.getMessage()), exception);
        }
    }
}
