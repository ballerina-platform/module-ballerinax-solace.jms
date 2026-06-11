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

package io.ballerina.lib.solace.smf.config;

import com.solace.messaging.config.SolaceConstants.AuthenticationConstants;
import com.solace.messaging.config.SolaceProperties.AuthenticationProperties;
import com.solace.messaging.config.SolaceProperties.ClientProperties;
import com.solace.messaging.config.SolaceProperties.ServiceProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerProperties;
import com.solace.messaging.config.SolaceProperties.TransportLayerSecurityProperties;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BString;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Builds the {@code java.util.Properties} used to construct a
 * {@link com.solace.messaging.MessagingService} from the Ballerina {@code smf:ConnectionConfiguration}.
 */
public final class ConnectionPropertiesBuilder {

    private static final BString MESSAGE_VPN = StringUtils.fromString("messageVpn");
    private static final BString AUTH = StringUtils.fromString("auth");
    private static final BString SECURE_SOCKET = StringUtils.fromString("secureSocket");
    private static final BString RETRY_CONFIG = StringUtils.fromString("retryConfig");
    private static final BString CLIENT_NAME = StringUtils.fromString("clientName");
    private static final BString APPLICATION_DESCRIPTION = StringUtils.fromString("applicationDescription");
    private static final BString CONNECT_TIMEOUT = StringUtils.fromString("connectTimeout");
    private static final BString COMPRESSION_LEVEL = StringUtils.fromString("compressionLevel");

    // BasicAuthConfig fields
    private static final BString USERNAME = StringUtils.fromString("username");
    private static final BString PASSWORD = StringUtils.fromString("password");
    // KerberosConfig fields
    private static final BString SERVICE_NAME = StringUtils.fromString("serviceName");
    private static final BString JAAS_LOGIN_CONTEXT = StringUtils.fromString("jaasLoginContext");
    private static final BString MUTUAL_AUTHENTICATION = StringUtils.fromString("mutualAuthentication");
    private static final BString JAAS_CONFIG_RELOAD_ENABLED = StringUtils.fromString("jaasConfigReloadEnabled");
    // OAuth2Config fields
    private static final BString ISSUER = StringUtils.fromString("issuer");
    private static final BString ACCESS_TOKEN = StringUtils.fromString("accessToken");
    private static final BString OIDC_TOKEN = StringUtils.fromString("oidcToken");
    // RetryConfig fields
    private static final BString CONNECT_RETRIES = StringUtils.fromString("connectRetries");
    private static final BString CONNECT_RETRIES_PER_HOST = StringUtils.fromString("connectRetriesPerHost");
    private static final BString RECONNECT_RETRIES = StringUtils.fromString("reconnectRetries");
    private static final BString RECONNECT_RETRY_WAIT = StringUtils.fromString("reconnectRetryWait");
    // SecureSocket fields
    private static final BString TRUST_STORE = StringUtils.fromString("trustStore");
    private static final BString KEY_STORE = StringUtils.fromString("keyStore");
    private static final BString EXCLUDED_PROTOCOLS = StringUtils.fromString("excludedProtocols");
    private static final BString CIPHER_SUITES = StringUtils.fromString("cipherSuites");
    private static final BString VALIDATION = StringUtils.fromString("validation");
    private static final BString LOCATION = StringUtils.fromString("location");
    private static final BString STORE_PASSWORD = StringUtils.fromString("password");
    private static final BString FORMAT = StringUtils.fromString("format");
    private static final BString KEY_PASSWORD = StringUtils.fromString("keyPassword");
    private static final BString KEY_ALIAS = StringUtils.fromString("keyAlias");
    private static final BString ENABLED = StringUtils.fromString("enabled");
    private static final BString VALIDATE_DATE = StringUtils.fromString("validateDate");
    private static final BString VALIDATE_HOST = StringUtils.fromString("validateHost");

    private ConnectionPropertiesBuilder() {}

    /**
     * Builds the messaging service properties from the broker URL and the connection configuration.
     *
     * @param url    Solace broker URL; {@code smf://}/{@code smfs://} schemes are translated to
     *               {@code tcp://}/{@code tcps://}
     * @param config Ballerina connection configuration map
     * @return properties for the {@code MessagingService} builder
     */
    public static Properties buildServiceProperties(String url, BMap<BString, Object> config) {
        Properties props = new Properties();
        props.setProperty(TransportLayerProperties.HOST, translateHostList(url));
        props.setProperty(ServiceProperties.VPN_NAME, config.getStringValue(MESSAGE_VPN).getValue());
        props.setProperty(TransportLayerProperties.CONNECTION_ATTEMPTS_TIMEOUT,
                Long.toString(decimalToMillis(((BDecimal) config.get(CONNECT_TIMEOUT)).decimalValue())));
        props.setProperty(TransportLayerProperties.COMPRESSION_LEVEL,
                Long.toString(config.getIntValue(COMPRESSION_LEVEL)));

        if (config.containsKey(CLIENT_NAME)) {
            props.setProperty(ClientProperties.NAME, config.getStringValue(CLIENT_NAME).getValue());
        }
        if (config.containsKey(APPLICATION_DESCRIPTION)) {
            props.setProperty(ClientProperties.APPLICATION_DESCRIPTION,
                    config.getStringValue(APPLICATION_DESCRIPTION).getValue());
        }

        if (config.containsKey(RETRY_CONFIG)) {
            addRetryProperties(props, (BMap<BString, Object>) config.getMapValue(RETRY_CONFIG));
        }

        BMap<BString, Object> secureSocket = config.containsKey(SECURE_SOCKET)
                ? (BMap<BString, Object>) config.getMapValue(SECURE_SOCKET) : null;
        if (config.containsKey(AUTH)) {
            addAuthProperties(props, (BMap<BString, Object>) config.getMapValue(AUTH));
        } else if (secureSocket != null && secureSocket.containsKey(KEY_STORE)) {
            // Client certificate authentication when a keyStore is present without explicit auth config
            props.setProperty(AuthenticationProperties.SCHEME,
                    AuthenticationConstants.AUTHENTICATION_SCHEME_CLIENT_CERT);
        }

        if (secureSocket != null) {
            addSecureSocketProperties(props, secureSocket);
        }
        return props;
    }

    private static void addAuthProperties(Properties props, BMap<BString, Object> auth) {
        if (auth.containsKey(USERNAME)) {
            props.setProperty(AuthenticationProperties.SCHEME,
                    AuthenticationConstants.AUTHENTICATION_SCHEME_BASIC);
            props.setProperty(AuthenticationProperties.SCHEME_BASIC_USER_NAME,
                    auth.getStringValue(USERNAME).getValue());
            if (auth.containsKey(PASSWORD)) {
                props.setProperty(AuthenticationProperties.SCHEME_BASIC_PASSWORD,
                        auth.getStringValue(PASSWORD).getValue());
            }
        } else if (auth.containsKey(SERVICE_NAME)) {
            props.setProperty(AuthenticationProperties.SCHEME,
                    AuthenticationConstants.AUTHENTICATION_SCHEME_KERBEROS);
            props.setProperty(AuthenticationProperties.SCHEME_KERBEROS_INSTANCE_NAME,
                    auth.getStringValue(SERVICE_NAME).getValue());
            props.setProperty(AuthenticationProperties.SCHEME_KERBEROS_JAAS_LOGIN_CONTEXT,
                    auth.getStringValue(JAAS_LOGIN_CONTEXT).getValue());
            props.setProperty(AuthenticationProperties.SCHEME_KERBEROS_MUTUAL_AUTH,
                    Boolean.toString(auth.getBooleanValue(MUTUAL_AUTHENTICATION)));
            props.setProperty(AuthenticationProperties.SCHEME_KERBEROS_JAAS_CONFIG_RELOADABLE,
                    Boolean.toString(auth.getBooleanValue(JAAS_CONFIG_RELOAD_ENABLED)));
        } else if (auth.containsKey(ISSUER)) {
            props.setProperty(AuthenticationProperties.SCHEME,
                    AuthenticationConstants.AUTHENTICATION_SCHEME_OAUTH2);
            props.setProperty(AuthenticationProperties.SCHEME_OAUTH2_ISSUER_IDENTIFIER,
                    auth.getStringValue(ISSUER).getValue());
            if (auth.containsKey(ACCESS_TOKEN)) {
                props.setProperty(AuthenticationProperties.SCHEME_OAUTH2_ACCESS_TOKEN,
                        auth.getStringValue(ACCESS_TOKEN).getValue());
            }
            if (auth.containsKey(OIDC_TOKEN)) {
                props.setProperty(AuthenticationProperties.SCHEME_OAUTH2_OIDC_ID_TOKEN,
                        auth.getStringValue(OIDC_TOKEN).getValue());
            }
        }
    }

    private static void addRetryProperties(Properties props, BMap<BString, Object> retry) {
        props.setProperty(TransportLayerProperties.CONNECTION_RETRIES,
                Long.toString(retry.getIntValue(CONNECT_RETRIES)));
        props.setProperty(TransportLayerProperties.CONNECTION_RETRIES_PER_HOST,
                Long.toString(retry.getIntValue(CONNECT_RETRIES_PER_HOST)));
        props.setProperty(TransportLayerProperties.RECONNECTION_ATTEMPTS,
                Long.toString(retry.getIntValue(RECONNECT_RETRIES)));
        props.setProperty(TransportLayerProperties.RECONNECTION_ATTEMPTS_WAIT_INTERVAL,
                Long.toString(decimalToMillis(((BDecimal) retry.get(RECONNECT_RETRY_WAIT)).decimalValue())));
    }

    private static void addSecureSocketProperties(Properties props, BMap<BString, Object> secureSocket) {
        if (secureSocket.containsKey(TRUST_STORE)) {
            BMap<BString, Object> trustStore = (BMap<BString, Object>) secureSocket.getMapValue(TRUST_STORE);
            props.setProperty(TransportLayerSecurityProperties.TRUST_STORE_PATH,
                    trustStore.getStringValue(LOCATION).getValue());
            props.setProperty(TransportLayerSecurityProperties.TRUST_STORE_PASSWORD,
                    trustStore.getStringValue(STORE_PASSWORD).getValue());
            props.setProperty(TransportLayerSecurityProperties.TRUST_STORE_TYPE,
                    trustStore.getStringValue(FORMAT).getValue().toUpperCase(Locale.ROOT));
        }

        if (secureSocket.containsKey(KEY_STORE)) {
            BMap<BString, Object> keyStore = (BMap<BString, Object>) secureSocket.getMapValue(KEY_STORE);
            props.setProperty(AuthenticationProperties.SCHEME_CLIENT_CERT_KEYSTORE,
                    keyStore.getStringValue(LOCATION).getValue());
            props.setProperty(AuthenticationProperties.SCHEME_CLIENT_CERT_KEYSTORE_PASSWORD,
                    keyStore.getStringValue(STORE_PASSWORD).getValue());
            props.setProperty(AuthenticationProperties.SCHEME_CLIENT_CERT_KEYSTORE_FORMAT,
                    keyStore.getStringValue(FORMAT).getValue().toUpperCase(Locale.ROOT));
            if (keyStore.containsKey(KEY_PASSWORD)) {
                props.setProperty(AuthenticationProperties.SCHEME_CLIENT_CERT_PRIVATE_KEY_PASSWORD,
                        keyStore.getStringValue(KEY_PASSWORD).getValue());
            }
            if (keyStore.containsKey(KEY_ALIAS)) {
                props.setProperty(AuthenticationProperties.SCHEME_CLIENT_CERT_PRIVATE_KEY_ALIAS,
                        keyStore.getStringValue(KEY_ALIAS).getValue());
            }
        }

        BMap<BString, Object> validation = (BMap<BString, Object>) secureSocket.getMapValue(VALIDATION);
        props.setProperty(TransportLayerSecurityProperties.CERT_VALIDATED,
                Boolean.toString(validation.getBooleanValue(ENABLED)));
        props.setProperty(TransportLayerSecurityProperties.CERT_REJECT_EXPIRED,
                Boolean.toString(validation.getBooleanValue(VALIDATE_DATE)));
        props.setProperty(TransportLayerSecurityProperties.CERT_VALIDATE_SERVERNAME,
                Boolean.toString(validation.getBooleanValue(VALIDATE_HOST)));

        BArray excludedProtocols = secureSocket.getArrayValue(EXCLUDED_PROTOCOLS);
        if (excludedProtocols != null && excludedProtocols.size() > 0) {
            props.setProperty(TransportLayerSecurityProperties.EXCLUDED_PROTOCOLS,
                    joinStringArray(excludedProtocols.getStringArray()));
        }

        if (secureSocket.containsKey(CIPHER_SUITES)) {
            BArray cipherSuites = secureSocket.getArrayValue(CIPHER_SUITES);
            if (cipherSuites.size() > 0) {
                props.setProperty(TransportLayerSecurityProperties.CIPHER_SUITES,
                        joinStringArray(cipherSuites.getStringArray()));
            }
        }
    }

    /**
     * Translates {@code smf://}/{@code smfs://} schemes used by the JMS surface to the
     * {@code tcp://}/{@code tcps://} schemes expected by the messaging API, for each entry
     * in a comma-separated host list.
     *
     * @param url broker URL, possibly a comma-separated host list
     * @return translated host list
     */
    public static String translateHostList(String url) {
        return Arrays.stream(url.split(","))
                .map(String::trim)
                .map(host -> {
                    if (host.startsWith("smfs://")) {
                        return "tcps://" + host.substring("smfs://".length());
                    }
                    if (host.startsWith("smf://")) {
                        return "tcp://" + host.substring("smf://".length());
                    }
                    return host;
                })
                .collect(Collectors.joining(","));
    }

    private static long decimalToMillis(BigDecimal seconds) {
        return seconds.multiply(BigDecimal.valueOf(1000)).longValue();
    }

    private static String joinStringArray(String[] values) {
        return String.join(",", values);
    }
}
