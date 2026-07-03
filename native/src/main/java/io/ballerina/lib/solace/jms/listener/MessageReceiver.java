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

package io.ballerina.lib.solace.jms.listener;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;

/**
 * A {MessageReceiver} registers itself as a JMS {@link MessageListener} on the underlying
 * {@link MessageConsumer} and dispatches messages pushed by the broker to the Solace service using the
 * message dispatcher, one message at a time.
 */
public class MessageReceiver implements MessageListener {
    private static final long stopTimeout = 30000;

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final Session session;
    private final MessageConsumer consumer;
    private final MessageDispatcher messageDispatcher;

    public MessageReceiver(Session session, MessageConsumer consumer, MessageDispatcher messageDispatcher) {
        this.session = session;
        this.consumer = consumer;
        this.messageDispatcher = messageDispatcher;
    }

    @Override
    public void onMessage(Message message) {
        if (closed.get()) {
            return;
        }
        Semaphore semaphore = new Semaphore(0);
        OnMsgCallback callback = new OnMsgCallback(semaphore);
        this.messageDispatcher.onMessage(message, callback);
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            this.messageDispatcher.onError(e);
            Thread.currentThread().interrupt();
        }
    }

    public void consume() throws JMSException {
        this.consumer.setMessageListener(this);
    }

    public void stop() {
        closed.set(true);
        // consumer.close() and session.close() both block until any in-progress onMessage() call
        // returns (JMS spec 4.5.2 / 4.3.2). Run both on the same bounded daemon thread so a stuck
        // Ballerina onMessage/onError handler cannot hang gracefulStop()/immediateStop()
        // indefinitely -- closing only the consumer on this thread would still leave session.close()
        // to block the caller directly.
        Thread closer = new Thread(() -> {
            try {
                this.consumer.close();
            } catch (JMSException e) {
                // Best-effort close.
            }
            try {
                this.session.close();
            } catch (JMSException e) {
                // Best-effort close.
            }
        }, "solace-jms-consumer-closer");
        closer.setDaemon(true);
        closer.start();
        try {
            closer.join(stopTimeout);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
