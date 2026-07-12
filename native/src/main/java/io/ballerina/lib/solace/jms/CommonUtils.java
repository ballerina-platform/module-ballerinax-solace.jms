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

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SupportedProperty;
import io.ballerina.lib.solace.jms.config.ConnectionConfiguration;
import io.ballerina.lib.solace.jms.config.auth.BasicAuthConfiguration;
import io.ballerina.lib.solace.jms.config.auth.KerberosConfiguration;
import io.ballerina.lib.solace.jms.config.auth.OAuth2Configuration;
import io.ballerina.lib.solace.jms.config.retry.RetryConfig;
import io.ballerina.lib.solace.jms.config.ssl.SecureSocketConfig;
import io.ballerina.lib.solace.jms.consumer.Durability;
import io.ballerina.runtime.api.creators.ErrorCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BError;

import java.util.Arrays;
import java.util.Hashtable;
import java.util.Objects;

import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;

/**
 * Common utility methods for Solace connector.
 */
public final class CommonUtils {

    private static final String SOLACE_ERROR = "Error";

    private CommonUtils() {}

    /**
     * Builds Solace JNDI connection properties from connection configuration.
     *
     * @param url    Solace broker URL
     * @param config Connection configuration
     * @return Hashtable of connection properties
     */
    public static Hashtable<String, Object> buildConnectionProperties(String url, ConnectionConfiguration config) {
        Hashtable<String, Object> props = new Hashtable<>();
        props.put(Context.PROVIDER_URL, url);
        props.put(SupportedProperty.SOLACE_JMS_VPN, config.messageVpn());
        props.put(SupportedProperty.SOLACE_JMS_DYNAMIC_DURABLES, config.enableDynamicDurables());

        if (config.clientName() != null) {
            props.put(SupportedProperty.SOLACE_JMS_JNDI_CLIENT_ID, config.clientName());
        }
        if (config.clientDescription() != null && !config.clientDescription().isEmpty()) {
            props.put(SupportedProperty.SOLACE_JMS_JNDI_CLIENT_DESCRIPTION, config.clientDescription());
        }

        props.put(SupportedProperty.SOLACE_JMS_JNDI_CONNECT_TIMEOUT, Math.toIntExact(config.connectTimeout()));
        props.put(SupportedProperty.SOLACE_JMS_JNDI_READ_TIMEOUT, Math.toIntExact(config.readTimeout()));
        props.put(SupportedProperty.SOLACE_JMS_COMPRESSION_LEVEL, config.compressionLevel());

        if (config.localhost() != null) {
            props.put(SupportedProperty.SOLACE_JMS_LOCALHOST, config.localhost());
        }

        if (config.retryConfig() != null) {
            RetryConfig retryConfig = config.retryConfig();
            props.put(SupportedProperty.SOLACE_JMS_JNDI_CONNECT_RETRIES, retryConfig.connectRetries());
            props.put(SupportedProperty.SOLACE_JMS_JNDI_CONNECT_RETRIES_PER_HOST,
                    retryConfig.connectRetriesPerHost());
            props.put(SupportedProperty.SOLACE_JMS_JNDI_RECONNECT_RETRIES, retryConfig.reconnectRetries());
            props.put(SupportedProperty.SOLACE_JMS_JNDI_RECONNECT_RETRY_WAIT,
                    Math.toIntExact(retryConfig.reconnectRetryWait()));
        }

        // Authentication priority: explicit auth config > client certificate (keyStore) > basic auth (default)
        if (config.auth() != null) {
            switch (config.auth()) {
                case BasicAuthConfiguration basic -> {
                    props.put(SupportedProperty.SOLACE_JMS_AUTHENTICATION_SCHEME,
                            SupportedProperty.AUTHENTICATION_SCHEME_BASIC);
                    props.put(Context.SECURITY_PRINCIPAL, basic.username());
                    if (basic.password() != null) {
                        props.put(Context.SECURITY_CREDENTIALS, basic.password());
                    }
                }
                case KerberosConfiguration kerberos -> {
                    props.put(SupportedProperty.SOLACE_JMS_AUTHENTICATION_SCHEME,
                            SupportedProperty.AUTHENTICATION_SCHEME_GSS_KRB);
                    props.put(SupportedProperty.SOLACE_JMS_KRB_MUTUAL_AUTHENTICATION,
                            kerberos.mutualAuthentication());
                    props.put(SupportedProperty.SOLACE_JMS_KRB_SERVICE_NAME, kerberos.serviceName());
                    if (kerberos.jaasLoginContext() != null) {
                        props.put(SupportedProperty.SOLACE_JMS_JAAS_LOGIN_CONTEXT, kerberos.jaasLoginContext());
                    }
                    props.put(SupportedProperty.SOLACE_JMS_JAAS_CONFIG_FILE_RELOAD_ENABLED,
                            kerberos.jaasConfigFileReloadEnabled());
                }
                case OAuth2Configuration oauth -> {
                    props.put(SupportedProperty.SOLACE_JMS_AUTHENTICATION_SCHEME,
                            SupportedProperty.AUTHENTICATION_SCHEME_OAUTH2);
                    props.put(SupportedProperty.SOLACE_JMS_OAUTH2_ISSUER_IDENTIFIER, oauth.issuer());
                    if (oauth.accessToken() != null) {
                        props.put(SupportedProperty.SOLACE_JMS_OAUTH2_ACCESS_TOKEN, oauth.accessToken());
                    }
                    if (oauth.oidcToken() != null) {
                        props.put(SupportedProperty.SOLACE_JMS_OIDC_ID_TOKEN, oauth.oidcToken());
                    }
                }
            }
        } else {
            props.put(SupportedProperty.SOLACE_JMS_AUTHENTICATION_SCHEME,
                    SupportedProperty.AUTHENTICATION_SCHEME_BASIC);
        }

        if (config.secureSocket() != null) {
            SecureSocketConfig sslConfig = config.secureSocket();

            if (sslConfig.trustStore() != null) {
                var trustStore = sslConfig.trustStore();
                props.put(SupportedProperty.SOLACE_JMS_SSL_TRUST_STORE, trustStore.location());
                props.put(SupportedProperty.SOLACE_JMS_SSL_TRUST_STORE_PASSWORD, trustStore.password());
                props.put(SupportedProperty.SOLACE_JMS_SSL_TRUST_STORE_FORMAT, trustStore.format());
            }

            if (sslConfig.keyStore() != null) {
                // Override to client certificate auth when keyStore is present without explicit auth config
                if (config.auth() == null) {
                    props.put(SupportedProperty.SOLACE_JMS_AUTHENTICATION_SCHEME,
                            SupportedProperty.AUTHENTICATION_SCHEME_CLIENT_CERTIFICATE);
                }
                var keyStore = sslConfig.keyStore();
                props.put(SupportedProperty.SOLACE_JMS_SSL_KEY_STORE, keyStore.location());
                props.put(SupportedProperty.SOLACE_JMS_SSL_KEY_STORE_PASSWORD, keyStore.password());
                props.put(SupportedProperty.SOLACE_JMS_SSL_KEY_STORE_FORMAT, keyStore.format());
                if (keyStore.keyPassword() != null) {
                    props.put(SupportedProperty.SOLACE_JMS_SSL_PRIVATE_KEY_PASSWORD, keyStore.keyPassword());
                }
                if (keyStore.keyAlias() != null) {
                    props.put(SupportedProperty.SOLACE_JMS_SSL_PRIVATE_KEY_ALIAS, keyStore.keyAlias());
                }
            }

            props.put(SupportedProperty.SOLACE_JMS_SSL_VALIDATE_CERTIFICATE, sslConfig.validation().enabled());
            props.put(SupportedProperty.SOLACE_JMS_SSL_VALIDATE_CERTIFICATE_DATE,
                    sslConfig.validation().validateDate());
            props.put(SupportedProperty.SOLACE_JMS_SSL_VALIDATE_CERTIFICATE_HOST,
                    sslConfig.validation().validateHostname());

            if (sslConfig.excludedProtocols() != null && !sslConfig.excludedProtocols().isEmpty()) {
                props.put(SupportedProperty.SOLACE_JMS_SSL_EXCLUDED_PROTOCOLS,
                        String.join(",", sslConfig.excludedProtocols()));
            }

            if (sslConfig.cipherSuites() != null && !sslConfig.cipherSuites().isEmpty()) {
                props.put(SupportedProperty.SOLACE_JMS_SSL_CIPHER_SUITES,
                        String.join(",", sslConfig.cipherSuites()));
            }

            if (sslConfig.trustedCommonNames() != null && !sslConfig.trustedCommonNames().isEmpty()) {
                props.put(SupportedProperty.SOLACE_JMS_SSL_TRUSTED_COMMON_NAME_LIST,
                        String.join(",", sslConfig.trustedCommonNames()));
            }
        }

        return props;
    }

    /**
     * Applies guaranteed-delivery (Assured Delivery) receive flow-control settings to the given connection
     * factory. These settings are only effective when the connection uses guaranteed (persistent) delivery,
     * i.e. when {@code directTransport} is {@code false}.
     *
     * @param factory the connection factory to configure
     * @param config  connection configuration containing the flow-control settings
     */
    public static void applyFlowControlSettings(SolConnectionFactory factory, ConnectionConfiguration config) {
        if (config.transportWindowSize() != null) {
            factory.setReceiveADWindowSize(config.transportWindowSize());
        }
        if (config.ackThreshold() != null) {
            factory.setReceiveAdAckThreshold(config.ackThreshold());
        }
        if (config.ackTimerMillis() != null) {
            factory.setReceiveADAckTimerInMillis(Math.toIntExact(config.ackTimerMillis()));
        }
    }

    /**
     * Creates a Ballerina error with given message.
     *
     * @param message error message
     * @return Ballerina error
     */
    public static BError createError(String message) {
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_ERROR,
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
        return ErrorCreator.createError(ModuleUtils.getModule(), SOLACE_ERROR,
                StringUtils.fromString(message), ErrorCreator.createError(cause), null);
    }

    /**
     * Creates a JMS MessageConsumer for a queue.
     *
     * @param session         JMS session
     * @param queueName       Queue name
     * @param messageSelector Optional message selector
     * @return JMS MessageConsumer
     * @throws JMSException if consumer creation fails
     */
    public static MessageConsumer createQueueConsumer(Session session, String queueName, String messageSelector)
            throws JMSException {
        return createConsumerForQueue(session, session.createQueue(queueName), messageSelector);
    }

    /**
     * Creates a {@link Queue} - either a durable, named queue, or a temporary (provider-named) one. Real JMS
     * temporary queues never take a caller-supplied name ({@link Session#createTemporaryQueue()} takes no
     * arguments) - a TEMPORARY durability with a queueName is rejected earlier, at Ballerina config validation
     * time.
     *
     * @param session     JMS session
     * @param queueName   Queue name (only used when durability is DURABLE)
     * @param durability  DURABLE or TEMPORARY
     * @return the created Queue
     * @throws JMSException if queue creation fails
     */
    public static Queue createQueue(Session session, String queueName, Durability durability) throws JMSException {
        return durability == Durability.TEMPORARY ? session.createTemporaryQueue() : session.createQueue(queueName);
    }

    /**
     * Creates a JMS MessageConsumer for an already-created queue.
     *
     * @param session         JMS session
     * @param queue           the queue to consume from
     * @param messageSelector Optional message selector
     * @return JMS MessageConsumer
     * @throws JMSException if consumer creation fails
     */
    public static MessageConsumer createConsumerForQueue(Session session, Queue queue, String messageSelector)
            throws JMSException {
        if (messageSelector != null && !messageSelector.isEmpty()) {
            return session.createConsumer(queue, messageSelector);
        } else {
            return session.createConsumer(queue);
        }
    }

    /**
     * Creates a JMS MessageConsumer for a topic.
     *
     * @param session         JMS session
     * @param topicName       Topic name
     * @param messageSelector Optional message selector
     * @param noLocal         No local flag
     * @param durability      Durability (TEMPORARY or DURABLE)
     * @param subscriberName  Subscriber name (required for DURABLE)
     * @return JMS MessageConsumer
     * @throws JMSException if consumer creation fails
     */
    public static MessageConsumer createTopicConsumer(Session session, String topicName, String messageSelector,
                                                      boolean noLocal, Durability durability,
                                                      String subscriberName) throws JMSException {
        Topic topic = session.createTopic(topicName);

        return switch (durability) {
            case TEMPORARY -> {
                if (messageSelector != null && !messageSelector.isEmpty()) {
                    yield session.createConsumer(topic, messageSelector, noLocal);
                } else {
                    yield session.createConsumer(topic);
                }
            }
            case DURABLE -> {
                if (subscriberName == null || subscriberName.isEmpty()) {
                    throw new IllegalArgumentException("Subscriber name is required for DURABLE consumer type");
                }
                if (messageSelector != null && !messageSelector.isEmpty()) {
                    yield session.createDurableSubscriber(topic, subscriberName, messageSelector, noLocal);
                } else {
                    yield session.createDurableSubscriber(topic, subscriberName);
                }
            }
        };
    }

    /**
     * Converts an array of Objects to an array of Strings.
     *
     * @param objectArray array of Objects
     * @return array of Strings
     */
    public static String[] convertToStringArray(Object[] objectArray) {
        if (Objects.isNull(objectArray)) {
            return new String[]{};
        }
        return Arrays.stream(objectArray)
                .filter(Objects::nonNull)
                .map(Object::toString)
                .toArray(String[]::new);
    }

    /**
     * Maps protocol names from Ballerina constants to Solace JMS expected values.
     *
     * @param protocols array of protocol names
     * @return mapped array of protocol names
     */
    public static String[] mapProtocols(String[] protocols) {
        if (protocols == null || protocols.length == 0) {
            return new String[]{};
        }
        return Arrays.stream(protocols).map(protocol -> {
            return switch (protocol) {
                case "TLSV1_1" -> "tlsv1.1";
                case "TLSV1_2" -> "tlsv1.2";
                case "TLSV1_3" -> "tlsv1.3";
                default -> throw new IllegalArgumentException("Unsupported protocol: " + protocol);
            };
        }).toArray(String[]::new);
    }
}
