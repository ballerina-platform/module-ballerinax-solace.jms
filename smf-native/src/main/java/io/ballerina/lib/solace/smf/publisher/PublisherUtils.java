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
import com.solace.messaging.config.PublisherBackPressureConfiguration;
import com.solace.messaging.config.profile.ConfigurationProfile;
import io.ballerina.lib.solace.smf.config.ConnectionPropertiesBuilder;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.util.Properties;

/**
 * Shared helpers for the SMF publisher actions.
 */
final class PublisherUtils {

    static final String NATIVE_MESSAGING_SERVICE = "native.smf.messaging.service";
    static final String NATIVE_PUBLISHER = "native.smf.publisher";
    static final long TERMINATE_GRACE_PERIOD_MILLIS = 10_000;

    private static final BString BACK_PRESSURE = StringUtils.fromString("backPressure");
    private static final BString STRATEGY = StringUtils.fromString("strategy");
    private static final BString BUFFER_CAPACITY = StringUtils.fromString("bufferCapacity");
    private static final String WAIT_WHEN_FULL = "WAIT_WHEN_FULL";
    private static final String REJECT_WHEN_FULL = "REJECT_WHEN_FULL";

    private PublisherUtils() {}

    static MessagingService connect(String url, BMap<BString, Object> config) {
        Properties props = ConnectionPropertiesBuilder.buildServiceProperties(url, config);
        return MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(props)
                .build()
                .connect();
    }

    /**
     * Applies the Ballerina {@code backPressure} configuration to a publisher builder.
     *
     * @param builder publisher builder (direct or persistent)
     * @param config  Ballerina publisher configuration map
     */
    static void applyBackPressure(PublisherBackPressureConfiguration builder, BMap<BString, Object> config) {
        BMap<BString, Object> backPressure = (BMap<BString, Object>) config.getMapValue(BACK_PRESSURE);
        String strategy = backPressure.getStringValue(STRATEGY).getValue();
        int bufferCapacity = backPressure.getIntValue(BUFFER_CAPACITY).intValue();
        switch (strategy) {
            case WAIT_WHEN_FULL -> builder.onBackPressureWait(bufferCapacity);
            case REJECT_WHEN_FULL -> builder.onBackPressureReject(bufferCapacity);
            default -> builder.onBackPressureElastic();
        }
    }
}
