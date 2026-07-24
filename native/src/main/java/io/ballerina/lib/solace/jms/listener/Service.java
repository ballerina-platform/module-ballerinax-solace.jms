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

import io.ballerina.lib.solace.jms.ModuleUtils;
import io.ballerina.runtime.api.types.RemoteMethodType;
import io.ballerina.runtime.api.types.ServiceType;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.utils.TypeUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;

import java.util.Optional;
import java.util.stream.Stream;

import static io.ballerina.runtime.api.constants.RuntimeConstants.ORG_NAME_SEPARATOR;
import static io.ballerina.runtime.api.constants.RuntimeConstants.VERSION_SEPARATOR;

/**
 * Native representation of a compiler-validated Ballerina Solace JMS service object.
 */
public class Service {
    private static final BString SERVICE_CONFIG_ANNOTATION = StringUtils.fromString(
            ModuleUtils.getModule().getOrg() + ORG_NAME_SEPARATOR +
                    ModuleUtils.getModule().getName() + VERSION_SEPARATOR +
                    ModuleUtils.getModule().getMajorVersion() + VERSION_SEPARATOR + "ServiceConfig");
    private static final BString QUEUE_NAME = StringUtils.fromString("queueName");
    private static final String ON_MSG_METHOD = "onMessage";
    private static final String ON_ERR_METHOD = "onError";

    private final BObject consumerService;
    private final ServiceType serviceType;
    private final ServiceConfig serviceConfig;
    private final RemoteMethodType onMessage;
    private final Optional<RemoteMethodType> onError;

    Service(BObject consumerService) {
        this.consumerService = consumerService;
        ServiceType svcType = (ServiceType) TypeUtils.getImpliedType(TypeUtils.getType(consumerService));
        this.serviceType = svcType;
        BMap<BString, Object> svcConfig = (BMap<BString, Object>) svcType.getAnnotation(SERVICE_CONFIG_ANNOTATION);
        this.serviceConfig = svcConfig.containsKey(QUEUE_NAME) ?
                new QueueConfig(svcConfig) : new TopicConfig(svcConfig);
        this.onMessage = Stream.of(svcType.getRemoteMethods())
                .filter(m -> ON_MSG_METHOD.equals(m.getName()))
                .findFirst().orElseThrow();
        this.onError = Stream.of(svcType.getRemoteMethods())
                .filter(m -> ON_ERR_METHOD.equals(m.getName()))
                .findFirst();
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

    public ServiceConfig getServiceConfig() {
        return serviceConfig;
    }

    public RemoteMethodType getOnMessageMethod() {
        return onMessage;
    }

    public Optional<RemoteMethodType> getOnError() {
        return onError;
    }
}
