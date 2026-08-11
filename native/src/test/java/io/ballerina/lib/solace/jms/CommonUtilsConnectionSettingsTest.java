/*
 * Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.ballerina.lib.solace.jms;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import io.ballerina.lib.solace.jms.config.ConnectionConfiguration;
import io.ballerina.lib.solace.jms.config.retry.RetryConfig;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Hashtable;

/**
 * Regression tests for issue #2012: several {@link ConnectionConfiguration} fields must reach the
 * {@link SolConnectionFactory} that producers, consumers, and listeners actually connect with, not just
 * a properties map that only a JNDI {@code InitialContext} lookup would ever read.
 */
public class CommonUtilsConnectionSettingsTest {

    private static final String BROKER_URL = "smf://localhost:55554";

    // Chosen to differ from every sol-jms SolConnectionFactory default, so a test that accidentally
    // asserts against the SDK's own default value cannot pass by coincidence.
    private static final String CLIENT_NAME = "unit-test-client-id";
    private static final String CLIENT_DESCRIPTION = "unit test client description";
    private static final long CONNECT_TIMEOUT_MILLIS = 12_345L;
    private static final long READ_TIMEOUT_MILLIS = 6_789L;
    private static final int CONNECT_RETRIES = 5;
    private static final int CONNECT_RETRIES_PER_HOST = 2;
    private static final int RECONNECT_RETRIES = 9;
    private static final long RECONNECT_RETRY_WAIT_MILLIS = 4_500L;

    private static ConnectionConfiguration testConfig() {
        RetryConfig retryConfig = new RetryConfig(
                CONNECT_RETRIES, CONNECT_RETRIES_PER_HOST, RECONNECT_RETRIES, RECONNECT_RETRY_WAIT_MILLIS);
        return new ConnectionConfiguration(
                "default", CLIENT_NAME, CLIENT_DESCRIPTION, false, true, true, null,
                CONNECT_TIMEOUT_MILLIS, READ_TIMEOUT_MILLIS, 0, null, retryConfig, null, null, 60, null);
    }

    private static SolConnectionFactory createFactory() throws Exception {
        ConnectionConfiguration config = testConfig();
        Hashtable<String, Object> connectionProps = CommonUtils.buildConnectionProperties(BROKER_URL, config);
        SolConnectionFactory factory = SolJmsUtility.createConnectionFactory(connectionProps);
        CommonUtils.applyConnectionFactorySettings(factory, config);
        return factory;
    }

    @Test
    public void testClientNameReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getClientID(), CLIENT_NAME);
    }

    @Test
    public void testClientDescriptionReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getClientDescription(), CLIENT_DESCRIPTION);
    }

    @Test
    public void testConnectTimeoutReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getConnectTimeoutInMillis().longValue(), CONNECT_TIMEOUT_MILLIS);
    }

    @Test
    public void testReadTimeoutReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getReadTimeoutInMillis().longValue(), READ_TIMEOUT_MILLIS);
    }

    @Test
    public void testConnectRetriesReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getConnectRetries().intValue(), CONNECT_RETRIES);
    }

    @Test
    public void testConnectRetriesPerHostReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getConnectRetriesPerHost().intValue(), CONNECT_RETRIES_PER_HOST);
    }

    @Test
    public void testReconnectRetriesReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getReconnectRetries().intValue(), RECONNECT_RETRIES);
    }

    @Test
    public void testReconnectRetryWaitReachesConnectionFactory() throws Exception {
        SolConnectionFactory factory = createFactory();
        Assert.assertEquals(factory.getReconnectRetryWaitInMillis().longValue(), RECONNECT_RETRY_WAIT_MILLIS);
    }
}
