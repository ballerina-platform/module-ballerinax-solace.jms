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
import com.solace.messaging.PersistentMessagePublisherBuilder;
import com.solace.messaging.PubSubPlusClientException;
import com.solace.messaging.publisher.OutboundMessage;
import com.solace.messaging.publisher.PersistentMessagePublisher;
import com.solace.messaging.resources.Topic;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;
import java.util.concurrent.CompletableFuture;

import static io.ballerina.lib.solace.smf.publisher.PublisherUtils.NATIVE_MESSAGING_SERVICE;
import static io.ballerina.lib.solace.smf.publisher.PublisherUtils.NATIVE_PUBLISHER;
import static io.ballerina.lib.solace.smf.publisher.PublisherUtils.TERMINATE_GRACE_PERIOD_MILLIS;

/**
 * Actions class for {@link PersistentMessagePublisher} with utility methods to invoke as inter-op functions.
 */
public final class PersistentPublisherActions {

    private PersistentPublisherActions() {}

    /**
     * Creates a {@link PersistentMessagePublisher} using the broker URL and publisher configurations.
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
            PersistentMessagePublisherBuilder builder = messagingService.createPersistentMessagePublisherBuilder();
            PublisherUtils.applyBackPressure(builder, config);
            PersistentMessagePublisher persistentPublisher = builder.build().start();
            publisher.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            publisher.addNativeData(NATIVE_PUBLISHER, persistentPublisher);
            return null;
        } catch (Exception exception) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(
                    String.format("Error occurred while initializing the Solace persistent message publisher: %s",
                            exception.getMessage()), exception);
        }
    }

    /**
     * Publishes a message to a topic with guaranteed delivery and blocks until the broker
     * acknowledges the message or the timeout elapses.
     *
     * @param publisher Ballerina publisher object
     * @param bMessage  Ballerina Solace message representation
     * @param topic     topic to publish to
     * @param timeout   maximum time to await the publish receipt, in seconds
     * @return {@code null} on success, or Ballerina {@code smf:Error} on failure
     */
    public static Object publish(BObject publisher, BMap<BString, Object> bMessage, BString topic, BDecimal timeout) {
        MessagingService messagingService = (MessagingService) publisher.getNativeData(NATIVE_MESSAGING_SERVICE);
        PersistentMessagePublisher persistentPublisher =
                (PersistentMessagePublisher) publisher.getNativeData(NATIVE_PUBLISHER);
        long timeoutMillis = timeout.decimalValue().multiply(BigDecimal.valueOf(1000)).longValue();

        CompletableFuture<Object> future = new CompletableFuture<>();
        Thread.startVirtualThread(() -> {
            try {
                OutboundMessage message = MessageConverter.toOutboundMessage(messagingService, bMessage);
                persistentPublisher.publishAwaitAcknowledgement(message, Topic.of(topic.getValue()), timeoutMillis);
                future.complete(null);
            } catch (PubSubPlusClientException.TimeoutException exception) {
                future.complete(CommonUtils.createError(
                        String.format("Timed out after %s seconds awaiting the publish receipt for topic '%s'",
                                timeout, topic.getValue()), exception));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                future.complete(CommonUtils.createError(
                        "Publish operation was interrupted while awaiting the publish receipt", exception));
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
        PersistentMessagePublisher persistentPublisher =
                (PersistentMessagePublisher) publisher.getNativeData(NATIVE_PUBLISHER);
        MessagingService messagingService = (MessagingService) publisher.getNativeData(NATIVE_MESSAGING_SERVICE);
        try {
            if (persistentPublisher != null) {
                persistentPublisher.terminate(TERMINATE_GRACE_PERIOD_MILLIS);
            }
            return null;
        } catch (Exception exception) {
            return CommonUtils.createError(
                    String.format("Error occurred while closing the persistent message publisher: %s",
                            exception.getMessage()), exception);
        } finally {
            // Always release the connection, even if terminate() failed, to avoid leaking it.
            CommonUtils.disconnectQuietly(messagingService);
        }
    }
}
