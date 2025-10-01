package com.example.kafka.connect.transforms;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.MapContext;
import org.apache.commons.jexl3.JexlExpression;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.nio.charset.StandardCharsets;

public class ConditionalDocumentFilter<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(ConditionalDocumentFilter.class);

    public static final String EXPR_PROPERTY = "couchbase.conditional.filter.expr";
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(EXPR_PROPERTY, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
                    ConfigDef.Importance.HIGH, "JEXL expression for conditional document filtering");

    private final ObjectMapper mapper = new ObjectMapper();
    private JexlExpression expression;

    @Override
    public R apply(R record) {
        if (record.value() == null) {
            log.info("Record value is null, skipping key={}", record.key());
            return null;
        }

        log.info("Applying filter to record key={} value type={}", record.key(), record.value().getClass());

        try {
            JsonNode docNode;
            Object value = record.value();

            // parse record value
            
            } else if (value instanceof String) {
                docNode = mapper.readTree((String) value);
                log.info("Parsed String record value");
            } else if (value instanceof Map) {
                docNode = mapper.valueToTree(value);
                log.info("Parsed Map record value");
            } else {
                log.warn("Unsupported record value type: {}", value.getClass());
                return record;
            }

            // flatten JSON into Map for JEXL
            Map<String, Object> flattenedMap = new HashMap<>();
            flattenJsonIntoMap(flattenedMap, docNode);
            JexlContext context = new MapContext(flattenedMap);

            log.info("Evaluating JEXL expression for record key={}", record.key());
            Boolean result = (Boolean) expression.evaluate(context);

            if (Boolean.FALSE.equals(result)) {
                log.info("Record filtered out by expression, key={}", record.key());
                return null;
            }

            // convert JsonNode back to Map for downstream SMTs
            Map<String, Object> mapValue = mapper.convertValue(docNode, Map.class);
            R newRecord = (R) record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    null,
                    mapValue,
                    record.timestamp()
            );

            log.info("Record passed filter, producing new record: key={}", record.key());
            return newRecord;

        } catch (Exception e) {
            log.error("Error applying filter to record key={}", record.key(), e);
            return null;
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to close
    }

    @Override
    public void configure(Map<String, ?> configs) {
        String exprString = (String) configs.get(EXPR_PROPERTY);
        if (exprString == null || exprString.isEmpty()) {
            throw new IllegalArgumentException(EXPR_PROPERTY + " configuration is required");
        }

        JexlEngine jexl = new JexlBuilder().create();
        this.expression = jexl.createExpression(exprString);
        log.info("Initialized conditional document filter with expression: {}", exprString);
    }

    // Flatten JSON into Map (objects as Maps, arrays as Lists, leaves as primitives)
    private void flattenJsonIntoMap(Map<String, Object> map, JsonNode node) {
        if (node.isObject()) {
            node.fieldNames().forEachRemaining(field -> {
                JsonNode child = node.get(field);
                if (child.isValueNode()) {
                    if (child.isTextual()) map.put(field, child.textValue());
                    else if (child.isNumber()) map.put(field, child.numberValue());
                    else if (child.isBoolean()) map.put(field, child.booleanValue());
                    else if (child.isNull()) map.put(field, null);
                } else if (child.isObject()) {
                    Map<String, Object> childMap = new HashMap<>();
                    map.put(field, childMap);
                    flattenJsonIntoMap(childMap, child);
                } else if (child.isArray()) {
                    List<Object> list = new ArrayList<>();
                    map.put(field, list);
                    for (JsonNode el : child) {
                        if (el.isValueNode()) {
                            if (el.isTextual()) list.add(el.textValue());
                            else if (el.isNumber()) list.add(el.numberValue());
                            else if (el.isBoolean()) list.add(el.booleanValue());
                            else if (el.isNull()) list.add(null);
                        } else if (el.isObject()) {
                            Map<String, Object> elMap = new HashMap<>();
                            list.add(elMap);
                            flattenJsonIntoMap(elMap, el);
                        } else if (el.isArray()) {
                            // nested arrays
                            List<Object> nestedList = new ArrayList<>();
                            list.add(nestedList);
                            flattenJsonIntoList(nestedList, el);
                        }
                    }
                }
            });
        }
    }

    // Helper for nested arrays
    private void flattenJsonIntoList(List<Object> list, JsonNode arrayNode) {
        for (JsonNode el : arrayNode) {
            if (el.isValueNode()) {
                if (el.isTextual()) list.add(el.textValue());
                else if (el.isNumber()) list.add(el.numberValue());
                else if (el.isBoolean()) list.add(el.booleanValue());
                else if (el.isNull()) list.add(null);
            } else if (el.isObject()) {
                Map<String, Object> elMap = new HashMap<>();
                list.add(elMap);
                flattenJsonIntoMap(elMap, el);
            } else if (el.isArray()) {
                List<Object> nestedList = new ArrayList<>();
                list.add(nestedList);
                flattenJsonIntoList(nestedList, el);
            }
        }
    }
}
