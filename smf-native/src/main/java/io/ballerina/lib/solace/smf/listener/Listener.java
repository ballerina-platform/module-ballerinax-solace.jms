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

import com.solace.messaging.MessagingService;
import com.solace.messaging.receiver.DirectMessageReceiver;
import com.solace.messaging.receiver.MessageReceiver;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.lib.solace.smf.receiver.ReceiverUtils;
import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Native class for the Ballerina Solace SMF Listener.
 */
public final class Listener {
    private static final String NATIVE_MESSAGING_SERVICE = "native.smf.messaging.service";
    private static final String NATIVE_SERVICE_LIST = "native.smf.service.list";
    private static final String NATIVE_SERVICE = "native.smf.service";
    private static final String NATIVE_RECEIVER = "native.smf.receiver";
    private static final String NATIVE_DISPATCHER = "native.smf.dispatcher";
    private static final String NATIVE_EXECUTOR = "native.smf.executor";
    private static final String LISTENER_STARTED = "smf.listener.started";

    private Listener() {}

    public static Object init(BObject bListener, BString url, BMap<BString, Object> config) {
        MessagingService messagingService = null;
        try {
            messagingService = CommonUtils.connect(url.getValue(), config);
            bListener.addNativeData(NATIVE_MESSAGING_SERVICE, messagingService);
            bListener.addNativeData(NATIVE_SERVICE_LIST, new ArrayList<BObject>());
            bListener.addNativeData(LISTENER_STARTED, new AtomicBoolean(false));
        } catch (Exception e) {
            CommonUtils.disconnectQuietly(messagingService);
            return CommonUtils.createError(String.format("Failed to initialize listener: %s", e.getMessage()), e);
        }
        return null;
    }

    public static Object attach(Environment env, BObject bListener, BObject bService, Object name) {
        MessagingService messagingService = (MessagingService) bListener.getNativeData(NATIVE_MESSAGING_SERVICE);
        AtomicBoolean started = (AtomicBoolean) bListener.getNativeData(LISTENER_STARTED);
        try {
            Service.validateService(env.getRuntime(), bService);
            Service nativeService = new Service(bService);
            BMap<BString, Object> svcConfig = nativeService.getServiceConfig();

            MessageReceiver receiver;
            PersistentMessageReceiver persistentReceiver = null;
            if (nativeService.isQueueSubscription()) {
                persistentReceiver = ReceiverUtils.buildPersistentReceiver(messagingService, svcConfig);
                receiver = persistentReceiver;
            } else {
                receiver = ReceiverUtils.buildDirectReceiver(messagingService, svcConfig);
            }
            MessageDispatcher dispatcher = new MessageDispatcher(
                    env.getRuntime(), nativeService, persistentReceiver);
            // A dedicated single-thread executor per service keeps message processing ordered for
            // the service while preventing one service's handler from blocking the others
            ExecutorService executor = Executors.newSingleThreadExecutor();

            bService.addNativeData(NATIVE_SERVICE, nativeService);
            bService.addNativeData(NATIVE_RECEIVER, receiver);
            bService.addNativeData(NATIVE_DISPATCHER, dispatcher);
            bService.addNativeData(NATIVE_EXECUTOR, executor);

            List<BObject> serviceList = (List<BObject>) bListener.getNativeData(NATIVE_SERVICE_LIST);
            serviceList.add(bService);

            if (started.get()) {
                startReceiver(bService);
            }
        } catch (BError e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return CommonUtils.createError(String.format("Failed to attach service to listener: %s", errorMsg), e);
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return CommonUtils.createError(String.format("Failed to attach service to listener: %s", errorMsg), e);
        }
        return null;
    }

    public static Object detach(BObject bListener, BObject bService) {
        try {
            stopReceiver(bService, ReceiverUtils.TERMINATE_GRACE_PERIOD_MILLIS);
            // Remove the service from the listener's list so a later start()/stop() does not act on a
            // detached, already-terminated service.
            List<BObject> serviceList = (List<BObject>) bListener.getNativeData(NATIVE_SERVICE_LIST);
            if (serviceList != null) {
                serviceList.remove(bService);
            }
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return CommonUtils.createError(
                    String.format("Failed to detach a service from the listener: %s", errorMsg), e);
        }
        return null;
    }

    public static Object start(BObject bListener) {
        List<BObject> bServices = (List<BObject>) bListener.getNativeData(NATIVE_SERVICE_LIST);
        AtomicBoolean started = (AtomicBoolean) bListener.getNativeData(LISTENER_STARTED);
        try {
            for (BObject bService: bServices) {
                startReceiver(bService);
            }
            started.set(true);
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return CommonUtils.createError(
                    String.format("Error occurred while starting the Ballerina Solace SMF listener: %s",
                            errorMsg), e);
        }
        return null;
    }

    public static Object gracefulStop(BObject bListener) {
        return stop(bListener, ReceiverUtils.TERMINATE_GRACE_PERIOD_MILLIS);
    }

    public static Object immediateStop(BObject bListener) {
        return stop(bListener, 0);
    }

    private static Object stop(BObject bListener, long gracePeriodMillis) {
        MessagingService messagingService = (MessagingService) bListener.getNativeData(NATIVE_MESSAGING_SERVICE);
        List<BObject> bServices = (List<BObject>) bListener.getNativeData(NATIVE_SERVICE_LIST);
        try {
            for (BObject bService: bServices) {
                stopReceiver(bService, gracePeriodMillis);
            }
            messagingService.disconnect();
        } catch (Exception e) {
            String errorMsg = Objects.isNull(e.getMessage()) ? "Unknown error" : e.getMessage();
            return CommonUtils.createError(
                    String.format("Error occurred while stopping the Ballerina Solace SMF listener: %s",
                            errorMsg), e);
        }
        return null;
    }

    private static void startReceiver(BObject bService) {
        MessageReceiver receiver = (MessageReceiver) bService.getNativeData(NATIVE_RECEIVER);
        MessageDispatcher dispatcher = (MessageDispatcher) bService.getNativeData(NATIVE_DISPATCHER);
        ExecutorService executor = (ExecutorService) bService.getNativeData(NATIVE_EXECUTOR);
        if (receiver instanceof PersistentMessageReceiver persistentReceiver) {
            persistentReceiver.start();
            persistentReceiver.receiveAsync(dispatcher, executor);
        } else {
            DirectMessageReceiver directReceiver = (DirectMessageReceiver) receiver;
            directReceiver.start();
            directReceiver.receiveAsync(dispatcher, executor);
        }
    }

    private static void stopReceiver(BObject bService, long gracePeriodMillis) {
        Object receiver = bService.getNativeData(NATIVE_RECEIVER);
        if (receiver instanceof MessageReceiver messageReceiver && !messageReceiver.isTerminated()) {
            messageReceiver.terminate(gracePeriodMillis);
        }
        Object executor = bService.getNativeData(NATIVE_EXECUTOR);
        if (executor instanceof ExecutorService executorService) {
            executorService.shutdown();
        }
    }
}
