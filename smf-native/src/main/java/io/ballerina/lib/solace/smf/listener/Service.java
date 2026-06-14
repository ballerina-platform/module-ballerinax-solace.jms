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

import io.ballerina.lib.solace.smf.CommonUtils;
import io.ballerina.lib.solace.smf.ModuleUtils;
import io.ballerina.lib.solace.smf.receiver.ReceiverUtils;
import io.ballerina.runtime.api.Runtime;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.types.Parameter;
import io.ballerina.runtime.api.types.RemoteMethodType;
import io.ballerina.runtime.api.types.ServiceType;
import io.ballerina.runtime.api.types.Type;
import io.ballerina.runtime.api.types.TypeTags;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static io.ballerina.runtime.api.constants.RuntimeConstants.ORG_NAME_SEPARATOR;
import static io.ballerina.runtime.api.constants.RuntimeConstants.VERSION_SEPARATOR;

/**
 * This is the native representation of the Ballerina Solace SMF service object.
 * This does the relevant configuration and method validation related to the SMF service. Ideally these validations
 * should be replaced by a compiler plugin.
 */
public class Service {
    private static final String IS_SMF_MSG_FUNCTION = "isSmfMessage";
    private static final Type CALLER_TYPE = ValueCreator.createObjectValue(
            ModuleUtils.getModule(), "Caller").getOriginalType();
    private static final Type ERROR_TYPE = TypeCreator.createErrorType("Error", ModuleUtils.getModule());
    private static final BString SERVICE_CONFIG_ANNOTATION = StringUtils.fromString(
            ModuleUtils.getModule().getOrg() + ORG_NAME_SEPARATOR +
                    ModuleUtils.getModule().getName() + VERSION_SEPARATOR +
                    ModuleUtils.getModule().getMajorVersion() + VERSION_SEPARATOR + "ServiceConfig");
    private static final String ON_MSG_METHOD = "onMessage";
    private static final String ON_ERR_METHOD = "onError";

    private final BObject consumerService;
    private final ServiceType serviceType;
    private final BMap<BString, Object> serviceConfig;
    private final RemoteMethodType onMessage;
    private final Optional<RemoteMethodType> onError;

    Service(BObject consumerService) {
        this.consumerService = consumerService;
        ServiceType svcType = (ServiceType) TypeUtils.getType(consumerService);
        this.serviceType = svcType;
        this.serviceConfig = (BMap<BString, Object>) svcType.getAnnotation(SERVICE_CONFIG_ANNOTATION);
        this.onMessage = Stream.of(svcType.getRemoteMethods())
                .filter(m -> ON_MSG_METHOD.equals(m.getName()))
                .findFirst().get();
        this.onError = Stream.of(svcType.getRemoteMethods())
                .filter(m -> ON_ERR_METHOD.equals(m.getName()))
                .findFirst();
    }

    public static void validateService(Runtime runtime, BObject consumerService) throws BError {
        ServiceType service = (ServiceType) TypeUtils.getType(consumerService);
        Object svcConfig = service.getAnnotation(SERVICE_CONFIG_ANNOTATION);
        if (Objects.isNull(svcConfig)) {
            throw CommonUtils.createError("Service configuration annotation is required.");
        }

        if (service.getResourceMethods().length > 0) {
            throw CommonUtils.createError("Solace SMF service cannot have resource methods.");
        }

        RemoteMethodType[] remoteMethods = service.getRemoteMethods();
        if (remoteMethods.length < 1 || remoteMethods.length > 2) {
            throw CommonUtils.createError("Solace SMF service must have exactly one or two remote methods.");
        }

        boolean isQueueSubscription = ReceiverUtils.isQueueSubscription((BMap<BString, Object>) svcConfig);
        boolean isAutoAck = ReceiverUtils.isAutoAck((BMap<BString, Object>) svcConfig);
        for (RemoteMethodType remoteMethod: remoteMethods) {
            String remoteMethodName = remoteMethod.getName();
            if (ON_MSG_METHOD.equals(remoteMethodName)) {
                validateOnMessageMethod(runtime, remoteMethod, isQueueSubscription, isAutoAck);
            } else if (ON_ERR_METHOD.equals(remoteMethodName)) {
                validateOnErrorMethod(remoteMethod);
            } else {
                throw CommonUtils.createError(String.format("Invalid remote method name: %s.", remoteMethodName));
            }
        }
    }

    private static void validateOnMessageMethod(Runtime runtime, RemoteMethodType onMessageMethod,
                                                boolean isQueueSubscription, boolean isAutoAck) {
        Parameter[] parameters = onMessageMethod.getParameters();
        if (parameters.length < 1 || parameters.length > 2) {
            throw CommonUtils.createError("onMessage method can only have either one or two parameters.");
        }

        Parameter message = null;
        boolean hasCaller = false;
        for (Parameter parameter : parameters) {
            Type parameterType = TypeUtils.getReferredType(parameter.type);
            if (isSubtypeOfSmfMessage(runtime, parameterType)) {
                message = parameter;
                continue;
            }
            if (TypeUtils.isSameType(CALLER_TYPE, parameterType)) {
                if (!isQueueSubscription) {
                    throw CommonUtils.createError(
                            "'smf:Caller' is not supported for direct topic subscriptions. " +
                                    "Message settlement is only available for persistent queue subscriptions.");
                }
                hasCaller = true;
                continue;
            }
            throw CommonUtils.createError(
                    "onMessage method parameters must be of type 'smf:Message' " +
                            "(or its subtype) or 'smf:Caller'.");
        }

        if (Objects.isNull(message)) {
            throw CommonUtils.createError("Required parameter 'smf:Message' can not be found.");
        }

        // A persistent queue service that does not auto-acknowledge must declare an 'smf:Caller' to
        // settle messages; otherwise messages are never acknowledged and are redelivered indefinitely.
        if (isQueueSubscription && !isAutoAck && !hasCaller) {
            throw CommonUtils.createError(
                    "A persistent queue service with 'autoAck' disabled must declare an 'smf:Caller' " +
                            "parameter in 'onMessage' to acknowledge messages; otherwise messages are " +
                            "never settled and are redelivered.");
        }
    }

    private static boolean isSubtypeOfSmfMessage(Runtime runtime, Type paramType) {
        if (paramType.getTag() != TypeTags.RECORD_TYPE_TAG && paramType.getTag() != TypeTags.INTERSECTION_TAG) {
            return false;
        }
        try {
            return (boolean) runtime.callFunction(ModuleUtils.getModule(), IS_SMF_MSG_FUNCTION, null,
                    ValueCreator.createTypedescValue(paramType));
        } catch (BError bError) {
            bError.printStackTrace();
            throw bError;
        }
    }

    private static void validateOnErrorMethod(RemoteMethodType onErrorMethod) {
        if (onErrorMethod.getParameters().length != 1) {
            throw CommonUtils.createError("onError method must have exactly one parameter of type 'smf:Error'.");
        }

        Parameter parameter = onErrorMethod.getParameters()[0];
        Type parameterType = TypeUtils.getReferredType(parameter.type);
        if (!TypeUtils.isSameType(ERROR_TYPE, parameterType)) {
            throw CommonUtils.createError("onError method parameter must be of type 'smf:Error'.");
        }
    }

    public boolean isOnMessageMethodIsolated() {
        return this.serviceType.isIsolated() && this.onMessage.isIsolated();
    }

    public boolean isOnErrorMethodIsolated() {
        return this.onError.map(m -> this.serviceType.isIsolated() && m.isIsolated()).orElse(false);
    }

    public BObject getConsumerService() {
        return consumerService;
    }

    public BMap<BString, Object> getServiceConfig() {
        return serviceConfig;
    }

    public boolean isQueueSubscription() {
        return ReceiverUtils.isQueueSubscription(serviceConfig);
    }

    public RemoteMethodType getOnMessageMethod() {
        return onMessage;
    }

    public Optional<RemoteMethodType> getOnError() {
        return onError;
    }
}
