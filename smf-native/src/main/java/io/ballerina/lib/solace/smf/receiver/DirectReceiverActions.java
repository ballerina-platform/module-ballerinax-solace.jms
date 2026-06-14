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
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.InboundMessage;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static io.ballerina.lib.solace.smf.receiver.ReceiverUtils.NATIVE_MESSAGING_SERVICE;
import static io.ballerina.lib.solace.smf.receiver.ReceiverUtils.NATIVE_RECEIVER;
import static io.ballerina.lib.solace.smf.receiver.ReceiverUtils.TERMINATE_GRACE_PERIOD_MILLIS;

/**
 * Actions class for {@link DirectMessageReceiver} with utility methods to invoke as inter-op functions.
 */
public final class DirectReceiverActions {

    private DirectReceiverActions() {}

    /**
     * Creates a {@link DirectMessageReceiver} using the broker URL and receiver configurations.
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
            DirectMessageReceiver directReceiver = ReceiverUtils.buildDirectReceiver(messagingService, config);
            directReceiver.start();
            receiver.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            receiver.addNativeData(NATIVE_RECEIVER, directReceiver);
            return null;
        } catch (Exception exception) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(
                    String.format("Error occurred while initializing the Solace direct message receiver: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Receives the next message from the subscribed topics within the specified timeout.
     *
     * @param receiver  Ballerina receiver object
     * @param timeout   timeout in seconds
     * @param bTypedesc expected message type
     * @return Ballerina message, {@code null} if no message available, or {@code smf:Error} on failure
     */
    public static Object receive(BObject receiver, BDecimal timeout, BTypedesc bTypedesc) {
        DirectMessageReceiver directReceiver = (DirectMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
        long timeoutMillis = timeout.decimalValue().multiply(BigDecimal.valueOf(1000)).longValue();

        CompletableFuture<Object> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                InboundMessage message = directReceiver.receiveMessage(timeoutMillis);
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
            return future.get();
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while waiting for operation to complete: %s",
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
            DirectMessageReceiver directReceiver = (DirectMessageReceiver) receiver.getNativeData(NATIVE_RECEIVER);
            MessagingService messagingService = (MessagingService) receiver.getNativeData(NATIVE_MESSAGING_SERVICE);
            if (directReceiver != null) {
                directReceiver.terminate(TERMINATE_GRACE_PERIOD_MILLIS);
            }
            if (messagingService != null) {
                messagingService.disconnect();
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while closing the direct message receiver: %s",
                            exception.getMessage()), exception);
        }
    }
}
