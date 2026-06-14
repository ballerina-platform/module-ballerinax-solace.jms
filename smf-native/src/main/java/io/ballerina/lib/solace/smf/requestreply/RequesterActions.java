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
import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.RequestReplyMessagePublisher;
import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.resources.Topic;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.lib.solace.smf.publisher.MessageConverter;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

/**
 * Actions class for {@link RequestReplyMessagePublisher} with utility methods to invoke as inter-op functions.
 */
public final class RequesterActions {

    private static final String NATIVE_MESSAGING_SERVICE = "native.smf.messaging.service";
    private static final String NATIVE_REQUESTER = "native.smf.requester";
    private static final long TERMINATE_GRACE_PERIOD_MILLIS = 10_000;

    private RequesterActions() {}

    /**
     * Creates a {@link RequestReplyMessagePublisher} using the broker URL and connection configurations.
     *
     * @param requester Ballerina requester object
     * @param url       Solace broker URL
     * @param config    Ballerina connection configurations
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object init(BObject requester, BString url, BMap<BString, Object> config) {
        MessagingService messagingService = null;
        try {
            messagingService = CommonUtils.connect(url.getValue(), config);
            RequestReplyMessagePublisher publisher = messagingService.requestReply()
                    .createRequestReplyMessagePublisherBuilder()
                    .build()
                    .start();
            requester.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            requester.addNativeData(NATIVE_REQUESTER, publisher);
            return null;
        } catch (Exception exception) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(
                    String.format("Error occurred while initializing the Solace message requester: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Publishes a request message to a topic and blocks until the reply arrives or the timeout elapses.
     *
     * @param requester Ballerina requester object
     * @param bMessage  Ballerina request message representation
     * @param topic     topic to publish the request to
     * @param timeout   maximum time to await the reply, in seconds
     * @param bTypedesc expected reply message type
     * @return the Ballerina reply message, or {@code smf:Error} on failure or timeout
     */
    public static Object request(BObject requester, BMap<BString, Object> bMessage, BString topic,
                                 BDecimal timeout, BTypedesc bTypedesc) {
        MessagingService messagingService = (MessagingService) requester.getNativeData(NATIVE_MESSAGING_SERVICE);
        RequestReplyMessagePublisher publisher =
                (RequestReplyMessagePublisher) requester.getNativeData(NATIVE_REQUESTER);
        long timeoutMillis = timeout.decimalValue().multiply(BigDecimal.valueOf(1000)).longValue();

        CompletableFuture<Object> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                OutboundMessage message = MessageConverter.toOutboundMessage(messagingService, bMessage);
                InboundMessage reply = publisher.publishAwaitResponse(
                        message, Topic.of(topic.getValue()), timeoutMillis);
                future.complete(io.ballerina.lib.solace.smf.receiver.MessageConverter
                        .toBallerinaMessage(reply, bTypedesc));
            } catch (PubSubPlusClientException.TimeoutException exception) {
                future.complete(CommonUtils.createError(
                        String.format("Timed out after %s seconds awaiting a reply on topic '%s'",
                                timeout, topic.getValue()), exception));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                future.complete(CommonUtils.createError(
                        "Request operation was interrupted while awaiting the reply", exception));
            } catch (Exception exception) {
                future.complete(CommonUtils.createError(
                        String.format("Error occurred while sending the request to topic '%s': %s",
                                topic.getValue(), exception.getMessage()), exception));
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
     * Terminates the requester and disconnects the underlying messaging service.
     *
     * @param requester Ballerina requester object
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object close(BObject requester) {
        try {
            RequestReplyMessagePublisher publisher =
                    (RequestReplyMessagePublisher) requester.getNativeData(NATIVE_REQUESTER);
            MessagingService messagingService = (MessagingService) requester.getNativeData(NATIVE_MESSAGING_SERVICE);
            if (publisher != null) {
                publisher.terminate(TERMINATE_GRACE_PERIOD_MILLIS);
            }
            if (messagingService != null) {
                messagingService.disconnect();
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while closing the message requester: %s",
                            exception.getMessage()), exception);
        }
    }
}
