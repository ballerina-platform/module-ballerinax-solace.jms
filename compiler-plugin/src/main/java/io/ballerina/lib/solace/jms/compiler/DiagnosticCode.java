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

package io.ballerina.lib.solace.jms.compiler;

/**
 * Diagnostics emitted by the Solace JMS compiler plugin.
 */
enum DiagnosticCode {
    MISSING_SERVICE_CONFIG("SOLACE_JMS_101", "service must have the ''solace.jms:ServiceConfig'' annotation"),
    RESOURCE_METHOD("SOLACE_JMS_102", "resource methods are not allowed in a ''solace.jms:Service''"),
    INVALID_REMOTE_METHOD_SET("SOLACE_JMS_103", "service must declare ''onMessage'' and may declare only one " +
            "optional ''onError'' remote method"),
    UNSUPPORTED_REMOTE_METHOD("SOLACE_JMS_104", "unsupported remote method ''{0}''; only ''onMessage'' and " +
            "''onError'' are allowed"),
    INVALID_ON_MESSAGE_PARAMETERS("SOLACE_JMS_105", "''onMessage'' must declare exactly one " +
            "''solace.jms:Message'' parameter followed by an optional ''solace.jms:Caller''"),
    INVALID_MESSAGE_PARAMETER("SOLACE_JMS_106", "parameter ''{0}'' must be ''solace.jms:Message'' (or a " +
            "supported subtype)"),
    INVALID_CALLER_PARAMETER("SOLACE_JMS_107", "the optional ''solace.jms:Caller'' parameter must be second"),
    INVALID_ON_ERROR_PARAMETER("SOLACE_JMS_108", "''onError'' must declare exactly one ''solace.jms:Error'' " +
            "parameter"),
    INVALID_RETURN_TYPE("SOLACE_JMS_109", "remote method ''{0}'' must return a value assignable to ''error?''"),
    MISSING_QUEUE_NAME("SOLACE_JMS_201", "queueName is required when the queue is DURABLE"),
    TEMPORARY_QUEUE_NAME("SOLACE_JMS_202", "queueName cannot be specified when the queue is TEMPORARY"),
    MISSING_SUBSCRIBER_NAME("SOLACE_JMS_203", "subscriberName is required when the topic is DURABLE");

    private final String code;
    private final String message;

    DiagnosticCode(String code, String message) {
        this.code = code;
        this.message = message;
    }

    String code() {
        return code;
    }

    String message() {
        return message;
    }
}
