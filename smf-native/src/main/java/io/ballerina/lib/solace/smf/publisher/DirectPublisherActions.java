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

import com.solace.messaging.DirectMessagePublisherBuilder;
import com.solace.messaging.MessagingService;
import com.solace.messaging.publisher.DirectMessagePublisher;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.resources.Topic;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.concurrent.CompletableFuture;

import static io.ballerina.lib.solace.smf.publisher.PublisherUtils.NATIVE_MESSAGING_SERVICE;
import static io.ballerina.lib.solace.smf.publisher.PublisherUtils.NATIVE_PUBLISHER;
import static io.ballerina.lib.solace.smf.publisher.PublisherUtils.TERMINATE_GRACE_PERIOD_MILLIS;

/**
 * Actions class for {@link DirectMessagePublisher} with utility methods to invoke as inter-op functions.
 */
public final class DirectPublisherActions {

    private DirectPublisherActions() {}

    /**
     * Creates a {@link DirectMessagePublisher} using the broker URL and publisher configurations.
     *
     * @param publisher Ballerina publisher object
     * @param url       Solace broker URL
     * @param config    Ballerina publisher configurations
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object init(BObject publisher, BString url, BMap<BString, Object> config) {
        MessagingService messagingService = null;
        try {
            messagingService = PublisherUtils.connect(url.getValue(), config);
            DirectMessagePublisherBuilder builder = messagingService.createDirectMessagePublisherBuilder();
            PublisherUtils.applyBackPressure(builder, config);
            DirectMessagePublisher directPublisher = builder.build().start();
            publisher.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            publisher.addNativeData(NATIVE_PUBLISHER, directPublisher);
            return null;
        } catch (Exception exception) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(
                    String.format("Error occurred while initializing the Solace direct message publisher: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Publishes a message to a topic using direct (at-most-once) delivery.
     *
     * @param publisher Ballerina publisher object
     * @param bMessage  Ballerina Solace message representation
     * @param topic     topic to publish to
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object publish(BObject publisher, BMap<BString, Object> bMessage, BString topic) {
        MessagingService messagingService = (MessagingService) publisher.getNativeData(NATIVE_MESSAGING_SERVICE);
        DirectMessagePublisher directPublisher = (DirectMessagePublisher) publisher.getNativeData(NATIVE_PUBLISHER);

        CompletableFuture<Object> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                OutboundMessage message = MessageConverter.toOutboundMessage(messagingService, bMessage);
                directPublisher.publish(message, Topic.of(topic.getValue()));
                future.complete(null);
            } catch (Exception exception) {
                future.complete(CommonUtils.createError(
                        String.format("Error occurred while publishing message to topic '%s': %s",
                                topic.getValue(), exception.getMessage()), exception));
            }
        });

        try {
            return future.get();
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while waiting for publish operation to complete: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Terminates the publisher and disconnects the underlying messaging service.
     *
     * @param publisher Ballerina publisher object
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object close(BObject publisher) {
        DirectMessagePublisher directPublisher =
                (DirectMessagePublisher) publisher.getNativeData(NATIVE_PUBLISHER);
        MessagingService messagingService = (MessagingService) publisher.getNativeData(NATIVE_MESSAGING_SERVICE);
        try {
            if (directPublisher != null) {
                directPublisher.terminate(TERMINATE_GRACE_PERIOD_MILLIS);
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while closing the direct message publisher: %s",
                            exception.getMessage()), exception);
        } finally {
            // Always release the connection, even if terminate() failed, to avoid leaking it.
            CommonUtils.disconnectQuietly(messagingService);
        }
    }
}
