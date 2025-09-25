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
            return null;
        }
        try {
            JsonNode docNode;
            Object value = record.value();

            if (value instanceof byte[]) {
                String jsonString = new String((byte[]) value, StandardCharsets.UTF_8);
                docNode = mapper.readTree(jsonString);
            } else if (value instanceof String) {
                docNode = mapper.readTree((String) value);
            } else if (value instanceof Map) {
                docNode = mapper.valueToTree(value);
            } else {
                log.info("Unsupported record value type: {}", value.getClass());
                return record;
            }

            // --- Flatten into JEXL context ---
            JexlContext context = new MapContext();
            flattenJson("", docNode, context);

            // INFO log of JEXL context
            log.info("JEXL Context Variables: {}", ((MapContext) context).getMap());

            // --- Evaluate Expression ---
            Boolean result = (Boolean) expression.evaluate(context);
            if (Boolean.FALSE.equals(result)) {
                log.info("Record filtered out by expression: {}", record.key());
                return null;
            }

            // Convert JsonNode back to Map for downstream SMTs
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

            // INFO log for final passing record
            log.info("Record passed filter: topic={} key={} value={}", 
                     newRecord.topic(), newRecord.key(), mapValue);

            return newRecord;

        } catch (Exception e) {
            log.info("Error applying filter to record: {}", record, e);
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

    // Helper method to flatten JSON into JEXL context
    private void flattenJson(String prefix, JsonNode node, JexlContext ctx) {
        if (node.isObject()) {
            if (!prefix.isEmpty()) {
                ctx.set(prefix, true); // mark object itself as present
            }
            node.fieldNames().forEachRemaining(fieldName -> {
                String key = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
                flattenJson(key, node.get(fieldName), ctx);
            });
        } else if (node.isArray()) {
            if (!prefix.isEmpty()) {
                ctx.set(prefix, true); // mark array itself as present
            }
            int index = 0;
            for (JsonNode element : node) {
                String key = prefix + "[" + index + "]";
                flattenJson(key, element, ctx);
                index++;
            }
        } else {
            // Leaf values only
            if (node.isBoolean()) {
                ctx.set(prefix, node.booleanValue());
            } else if (node.isNumber()) {
                ctx.set(prefix, node.numberValue());
            } else if (node.isTextual()) {
                ctx.set(prefix, node.textValue());
            } else if (node.isNull()) {
                ctx.set(prefix, null);
            }
        }
    }
}
