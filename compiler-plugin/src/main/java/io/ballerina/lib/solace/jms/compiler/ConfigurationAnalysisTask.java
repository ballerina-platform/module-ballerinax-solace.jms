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

import io.ballerina.compiler.api.symbols.ConstantSymbol;
import io.ballerina.compiler.api.symbols.Symbol;
import io.ballerina.compiler.api.symbols.TypeSymbol;
import io.ballerina.compiler.syntax.tree.ExpressionNode;
import io.ballerina.compiler.syntax.tree.MappingConstructorExpressionNode;
import io.ballerina.compiler.syntax.tree.MappingFieldNode;
import io.ballerina.compiler.syntax.tree.SpecificFieldNode;
import io.ballerina.compiler.syntax.tree.SyntaxKind;
import io.ballerina.projects.Document;
import io.ballerina.projects.plugins.AnalysisTask;
import io.ballerina.projects.plugins.SyntaxNodeAnalysisContext;
import io.ballerina.tools.diagnostics.Location;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Validates statically known Solace JMS queue and topic configurations.
 */
final class ConfigurationAnalysisTask implements AnalysisTask<SyntaxNodeAnalysisContext> {

    @Override
    public void perform(SyntaxNodeAnalysisContext context) {
        MappingConstructorExpressionNode mapping = (MappingConstructorExpressionNode) context.node();
        Optional<TypeSymbol> expectedType = expectedType(context, mapping);
        Optional<TypeSymbol> actualType = context.semanticModel().typeOf(mapping);
        TypeSymbol configurationType = selectConfigurationType(expectedType, actualType).orElse(null);
        if (configurationType == null) {
            return;
        }

        Map<String, SpecificFieldNode> fields = new HashMap<>();
        for (MappingFieldNode field : mapping.fields()) {
            if (field.kind() == SyntaxKind.SPREAD_FIELD) {
                return;
            }
            if (field instanceof SpecificFieldNode specificField) {
                fields.put(fieldName(specificField), specificField);
            }
        }

        boolean topic = PluginUtils.isSolaceJmsType(configurationType, "TopicConfiguration") ||
                PluginUtils.isSolaceJmsType(configurationType, "TopicServiceConfiguration") ||
                fields.containsKey("topicName");
        if (topic) {
            validateTopic(context, mapping, fields);
        } else {
            validateQueue(context, mapping, fields);
        }
    }

    private void validateQueue(SyntaxNodeAnalysisContext context, MappingConstructorExpressionNode mapping,
                               Map<String, SpecificFieldNode> fields) {
        Optional<String> durability = value(context, expression(fields.get("durability")));
        if (fields.containsKey("durability") && durability.isEmpty()) {
            return;
        }

        SpecificFieldNode queueNameField = fields.get("queueName");
        if (durability.orElse("DURABLE").equals("TEMPORARY")) {
            knownNonEmpty(context, queueNameField).ifPresent(nonEmpty -> {
                if (nonEmpty) {
                    report(context, DiagnosticCode.TEMPORARY_QUEUE_NAME, fieldLocation(queueNameField));
                }
            });
            return;
        }

        if (queueNameField == null) {
            report(context, DiagnosticCode.MISSING_QUEUE_NAME,
                    knownFieldLocation(fields.get("durability"), mapping));
            return;
        }
        knownNonEmpty(context, queueNameField).ifPresent(nonEmpty -> {
            if (!nonEmpty) {
                report(context, DiagnosticCode.MISSING_QUEUE_NAME, fieldLocation(queueNameField));
            }
        });
    }

    private void validateTopic(SyntaxNodeAnalysisContext context, MappingConstructorExpressionNode mapping,
                               Map<String, SpecificFieldNode> fields) {
        Optional<String> durability = value(context, expression(fields.get("durability")));
        if (!fields.containsKey("durability") || durability.isEmpty() || !durability.get().equals("DURABLE")) {
            return;
        }

        SpecificFieldNode subscriberNameField = fields.get("subscriberName");
        if (subscriberNameField == null) {
            report(context, DiagnosticCode.MISSING_SUBSCRIBER_NAME,
                    knownFieldLocation(fields.get("durability"), mapping));
            return;
        }
        knownNonEmpty(context, subscriberNameField).ifPresent(nonEmpty -> {
            if (!nonEmpty) {
                report(context, DiagnosticCode.MISSING_SUBSCRIBER_NAME, fieldLocation(subscriberNameField));
            }
        });
    }

    private Optional<Boolean> knownNonEmpty(SyntaxNodeAnalysisContext context, SpecificFieldNode field) {
        if (field == null) {
            return Optional.empty();
        }
        return value(context, expression(field)).map(value -> !value.isEmpty());
    }

    private Optional<TypeSymbol> selectConfigurationType(Optional<TypeSymbol> expected, Optional<TypeSymbol> actual) {
        return expected.filter(this::isConfigurationType).or(() -> actual.filter(this::isConfigurationType));
    }

    private boolean isConfigurationType(TypeSymbol type) {
        return PluginUtils.isSolaceJmsType(type, "QueueConfiguration") ||
                PluginUtils.isSolaceJmsType(type, "TopicConfiguration") ||
                PluginUtils.isSolaceJmsType(type, "SubscriptionConfiguration") ||
                PluginUtils.isSolaceJmsType(type, "QueueServiceConfiguration") ||
                PluginUtils.isSolaceJmsType(type, "TopicServiceConfiguration") ||
                PluginUtils.isSolaceJmsType(type, "ServiceConfiguration");
    }

    private Optional<TypeSymbol> expectedType(SyntaxNodeAnalysisContext context,
                                              MappingConstructorExpressionNode mapping) {
        Document document = context.currentPackage().module(context.moduleId()).document(context.documentId());
        return context.semanticModel().expectedType(document, mapping.location().lineRange().startLine());
    }

    private Optional<String> value(SyntaxNodeAnalysisContext context, ExpressionNode expression) {
        if (expression == null) {
            return Optional.empty();
        }
        String source = expression.toSourceCode().trim();
        if (expression.kind() == SyntaxKind.STRING_LITERAL) {
            return Optional.of(source.substring(1, source.length() - 1));
        }
        Optional<Symbol> symbol = context.semanticModel().symbol(expression);
        if (symbol.isPresent() && symbol.get() instanceof ConstantSymbol constant) {
            return constant.resolvedValue().map(this::stripQuotes);
        }
        if (source.equals("DURABLE") || source.endsWith(":DURABLE")) {
            return Optional.of("DURABLE");
        }
        if (source.equals("TEMPORARY") || source.endsWith(":TEMPORARY")) {
            return Optional.of("TEMPORARY");
        }
        return Optional.empty();
    }

    private ExpressionNode expression(SpecificFieldNode field) {
        return field == null ? null : field.valueExpr().orElse(null);
    }

    private Location fieldLocation(SpecificFieldNode field) {
        return field.valueExpr().map(ExpressionNode::location).orElse(field.location());
    }

    private Location knownFieldLocation(SpecificFieldNode field, MappingConstructorExpressionNode mapping) {
        return field == null ? mapping.location() : fieldLocation(field);
    }

    private void report(SyntaxNodeAnalysisContext context, DiagnosticCode code, Location location) {
        context.reportDiagnostic(PluginUtils.diagnostic(code, location));
    }

    private String fieldName(SpecificFieldNode field) {
        return stripQuotes(field.fieldName().toSourceCode().trim());
    }

    private String stripQuotes(String value) {
        if (value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
            return value.substring(1, value.length() - 1);
        }
        return value;
    }
}
