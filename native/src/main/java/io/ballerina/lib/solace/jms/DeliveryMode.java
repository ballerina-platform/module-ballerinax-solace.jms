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

package io.ballerina.lib.solace.jms;

/**
 * JMS message delivery modes.
 */
public enum DeliveryMode {
    NON_PERSISTENT(javax.jms.DeliveryMode.NON_PERSISTENT),
    PERSISTENT(javax.jms.DeliveryMode.PERSISTENT);

    private final int jmsMode;

    DeliveryMode(int jmsMode) {
        this.jmsMode = jmsMode;
    }

    /**
     * Returns the JMS delivery mode constant.
     *
     * @return JMS delivery mode
     */
    public int getJmsMode() {
        return jmsMode;
    }

    /**
     * Converts Ballerina delivery mode string to JMS delivery mode.
     *
     * @param mode Ballerina delivery mode
     * @return JMS delivery mode constant
     */
    public static int fromString(String mode) {
        return valueOf(mode).getJmsMode();
    }

    /**
     * Converts a JMS delivery mode constant back to its Ballerina enum member name.
     *
     * @param jmsMode JMS delivery mode constant
     * @return the matching Ballerina delivery mode name
     */
    public static String fromJmsMode(int jmsMode) {
        for (DeliveryMode mode : values()) {
            if (mode.jmsMode == jmsMode) {
                return mode.name();
            }
        }
        throw new IllegalArgumentException("Unknown JMS delivery mode: " + jmsMode);
    }
}
