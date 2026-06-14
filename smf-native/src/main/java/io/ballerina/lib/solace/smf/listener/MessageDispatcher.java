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

import com.solace.messaging.receiver.InboundMessage;
import com.solace.messaging.receiver.MessageReceiver;
import com.solace.messaging.receiver.PersistentMessageReceiver;
import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.lib.solace.smf.ModuleUtils;
import io.ballerina.lib.solace.smf.receiver.MessageConverter;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.concurrent.StrandMetadata;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.RemoteMethodType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;

import java.io.PrintStream;
import java.util.Optional;

/**
 * A {MessageDispatcher} dispatches Solace SMF messages into the SMF service using the
 * messaging API's native asynchronous receive callback.
 */
public class MessageDispatcher implements MessageReceiver.MessageHandler {
    private static final PrintStream ERR_OUT = System.err;
    private static final String ON_ERROR_METHOD = "onError";
    private static final String ON_MESSAGE_METHOD = "onMessage";
    private static final String BCALLER_NAME = "Caller";

    private final Runtime ballerinaRuntime;
    private final Service nativeService;
    private final PersistentMessageReceiver persistentReceiver;

    MessageDispatcher(Runtime ballerinaRuntime, Service nativeService, PersistentMessageReceiver persistentReceiver) {
        this.ballerinaRuntime = ballerinaRuntime;
        this.nativeService = nativeService;
        this.persistentReceiver = persistentReceiver;
    }

    /**
     * Handles a message delivered by the messaging API. This runs on the dedicated dispatch
     * executor of the owning service; blocking here serializes message processing per service
     * without affecting other services attached to the same listener.
     *
     * @param message the received message
     */
    @Override
    public void onMessage(InboundMessage message) {
        try {
            boolean isConcurrentSafe = nativeService.isOnMessageMethodIsolated();
            StrandMetadata metadata = new StrandMetadata(isConcurrentSafe, null);
            Object[] params = getOnMessageParams(message);
            Object result = ballerinaRuntime.callMethod(
                    nativeService.getConsumerService(), ON_MESSAGE_METHOD, metadata, params);
            if (result instanceof BError bError) {
                onError(bError);
            }
        } catch (BError bError) {
            onError(bError);
        } catch (Exception e) {
            onError(CommonUtils.createError(
                    "Unexpected error occurred while dispatching the message: " + e.getMessage(), e));
        }
    }

    private Object[] getOnMessageParams(InboundMessage message) throws Exception {
        Parameter[] parameters = this.nativeService.getOnMessageMethod().getParameters();
        Object[] args = new Object[parameters.length];
        int idx = 0;
        for (Parameter param : parameters) {
            Type referredType = TypeUtils.getReferredType(param.type);
            switch (referredType.getTag()) {
                case TypeTags.OBJECT_TYPE_TAG:
                    args[idx++] = getCaller();
                    break;
                default:
                    args[idx++] = MessageConverter
                            .toBallerinaMessage(message, ValueCreator.createTypedescValue(referredType));
                    break;
            }
        }
        return args;
    }

    private BObject getCaller() {
        BObject caller = ValueCreator.createObjectValue(ModuleUtils.getModule(), BCALLER_NAME);
        caller.addNativeData(Caller.NATIVE_RECEIVER, persistentReceiver);
        return caller;
    }

    public void onError(Throwable t) {
        Thread.startVirtualThread(() -> {
            try {
                ERR_OUT.println("Unexpected error occurred while message processing: " + t.getMessage());
                Optional<RemoteMethodType> onError = nativeService.getOnError();
                if (onError.isEmpty()) {
                    t.printStackTrace();
                    return;
                }
                BError error = t instanceof BError bError
                        ? bError : CommonUtils.createError("Failed to process the message", t);
                boolean isConcurrentSafe = nativeService.isOnErrorMethodIsolated();
                StrandMetadata metadata = new StrandMetadata(isConcurrentSafe, null);
                ballerinaRuntime.callMethod(
                        nativeService.getConsumerService(), ON_ERROR_METHOD, metadata, error);
            } catch (BError err) {
                err.printStackTrace();
            }
        });
    }
}
