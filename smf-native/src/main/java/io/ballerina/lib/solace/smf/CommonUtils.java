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

package io.ballerina.lib.solace.smf;

import com.solace.messaging.MessagingService;
import com.solace.messaging.config.profile.ConfigurationProfile;
import io.ballerina.lib.solace.smf.config.ConnectionPropertiesBuilder;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.io.PrintStream;
import java.util.Properties;

/**
 * Common utility methods for the Solace SMF module.
 */
public final class CommonUtils {

    private static final String SOLACE_SMF_ERROR = "Error";
    private static final PrintStream ERR_OUT = System.err;

    private CommonUtils() {}

    /**
     * Builds and connects a {@link MessagingService} from the broker URL and connection configuration.
     *
     * @param url    Solace broker URL
     * @param config Ballerina connection configuration map
     * @return the connected messaging service
     */
    public static MessagingService connect(String url, BMap<BString, Object> config) {
        Properties props = ConnectionPropertiesBuilder.buildServiceProperties(url, config);
        return MessagingService.builder(ConfigurationProfile.V1)
                .fromProperties(props)
                .build()
                .connect();
    }

    /**
     * Disconnects a {@link MessagingService}, ignoring any error. Used to release a connection that
     * was established but could not be fully initialized (e.g. building or starting the
     * publisher/receiver failed), so a failed initialization does not leak a broker connection.
     *
     * @param messagingService the messaging service to disconnect; a {@code null} is ignored
     */
    public static void disconnectQuietly(MessagingService messagingService) {
        if (messagingService == null) {
            return;
        }
        try {
            messagingService.disconnect();
        } catch (Exception exception) {
            // Best-effort cleanup; the original initialization error is what the caller reports.
            ERR_OUT.println("Failed to disconnect the messaging service during cleanup: "
                    + exception.getMessage());
        }
    }

    /**
     * Creates a Ballerina error with given message.
     *
     * @param message error message
     * @return Ballerina error
     */
    public static BError createError(String message) {
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_SMF_ERROR,
                StringUtils.fromString(message), null, null);
    }

    /**
     * Creates a Ballerina error with given message and cause.
     *
     * @param message error message
     * @param cause   exception cause
     * @return Ballerina error
     */
    public static BError createError(String message, Throwable cause) {
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_SMF_ERROR,
                StringUtils.fromString(message), ErrorCreator.createError(cause), null);
    }
}
