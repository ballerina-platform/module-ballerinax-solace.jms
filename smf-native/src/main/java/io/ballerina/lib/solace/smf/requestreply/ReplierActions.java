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

package io.ballerina.lib.solace.smf.requestreply;

import com.solace.messaging.MessagingService;
import com.solace.messaging.RequestReplyMessageReceiverBuilder;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.receiver.RequestReplyMessageReceiver;
import com.solace.messaging.receiver.RequestReplyMessageReceiver.Replier;
import com.solace.messaging.resources.ShareName;
import com.solace.messaging.resources.TopicSubscription;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.lib.solace.smf.receiver.MessageConverter;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Actions class for {@link RequestReplyMessageReceiver} with utility methods to invoke as inter-op functions.
 */
public final class ReplierActions {

    private static final String NATIVE_MESSAGING_SERVICE = "native.smf.messaging.service";
    private static final String NATIVE_REPLIER_RECEIVER = "native.smf.replier.receiver";
    private static final String NATIVE_REPLIER_HANDLE = "native.smf.replier.handle";
    private static final long TERMINATE_GRACE_PERIOD_MILLIS = 10_000;

    private static final BString TOPIC_SUBSCRIPTION = StringUtils.fromString("topicSubscription");
    private static final BString SHARE_NAME = StringUtils.fromString("shareName");

    private ReplierActions() {}

    /**
     * Creates a {@link RequestReplyMessageReceiver} using the broker URL and replier configurations.
     *
     * @param replier Ballerina replier object
     * @param url     Solace broker URL
     * @param config  Ballerina replier configurations
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object init(BObject replier, BString url, BMap<BString, Object> config) {
        MessagingService messagingService = null;
        try {
            messagingService = CommonUtils.connect(url.getValue(), config);
            RequestReplyMessageReceiverBuilder builder = messagingService.requestReply()
                    .createRequestReplyMessageReceiverBuilder();
            TopicSubscription subscription =
                    TopicSubscription.of(config.getStringValue(TOPIC_SUBSCRIPTION).getValue());
            RequestReplyMessageReceiver receiver = config.containsKey(SHARE_NAME)
                    ? builder.build(subscription, ShareName.of(config.getStringValue(SHARE_NAME).getValue()))
                    : builder.build(subscription);
            receiver.start();
            replier.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            replier.addNativeData(NATIVE_REPLIER_RECEIVER, receiver);
            return null;
        } catch (Exception exception) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(
                    String.format("Error occurred while initializing the Solace message replier: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Receives the next request message, waiting up to the specified timeout. The reply handle is
     * stored on the returned message so the reply can be sent later via {@code reply}.
     *
     * @param replier   Ballerina replier object
     * @param timeout   timeout in seconds
     * @param bTypedesc expected request message type
     * @return Ballerina message, {@code null} if no request arrives, or {@code smf:Error} on failure
     */
    public static Object receive(BObject replier, BDecimal timeout, BTypedesc bTypedesc) {
        RequestReplyMessageReceiver receiver =
                (RequestReplyMessageReceiver) replier.getNativeData(NATIVE_REPLIER_RECEIVER);
        long timeoutMillis = timeout.decimalValue().multiply(BigDecimal.valueOf(1000)).longValue();

        CompletableFuture<Object> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                AtomicReference<Object> received = new AtomicReference<>();
                receiver.receiveMessage((message, replierHandle) -> {
                    try {
                        BMap<BString, Object> ballerinaMessage =
                                MessageConverter.toBallerinaMessage(message, bTypedesc);
                        ballerinaMessage.addNativeData(NATIVE_REPLIER_HANDLE, replierHandle);
                        received.set(ballerinaMessage);
                    } catch (Exception e) {
                        received.set(CommonUtils.createError(e.getMessage(), e));
                    }
                }, timeoutMillis);
                future.complete(received.get());
            } catch (Exception exception) {
                future.complete(CommonUtils.createError(
                        String.format("Error occurred while receiving a request message: %s",
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
     * Sends a reply for a previously received request message.
     *
     * @param replier  Ballerina replier object
     * @param request  the received request message carrying the reply handle
     * @param response the Ballerina reply message representation
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object reply(BObject replier, BMap<BString, Object> request, BMap<BString, Object> response) {
        MessagingService messagingService = (MessagingService) replier.getNativeData(NATIVE_MESSAGING_SERVICE);
        Object replierHandle = request.getNativeData(NATIVE_REPLIER_HANDLE);
        if (replierHandle == null) {
            return CommonUtils.createError(
                    "Cannot send the reply: the request message does not carry a reply handle");
        }
        try {
            OutboundMessage outboundMessage = io.ballerina.lib.solace.smf.publisher.MessageConverter
                    .toOutboundMessage(messagingService, response);
            ((Replier) replierHandle).reply(outboundMessage);
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while sending the reply: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Terminates the replier and disconnects the underlying messaging service.
     *
     * @param replier Ballerina replier object
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object close(BObject replier) {
        try {
            RequestReplyMessageReceiver receiver =
                    (RequestReplyMessageReceiver) replier.getNativeData(NATIVE_REPLIER_RECEIVER);
            MessagingService messagingService = (MessagingService) replier.getNativeData(NATIVE_MESSAGING_SERVICE);
            if (receiver != null) {
                receiver.terminate(TERMINATE_GRACE_PERIOD_MILLIS);
            }
            if (messagingService != null) {
                messagingService.disconnect();
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while closing the message replier: %s",
                            exception.getMessage()), exception);
        }
    }
}
