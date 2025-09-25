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
            log.info("Record value is null, skipping key={}", record.key());
            return null;
        }

        log.info("Applying filter to record key={} value type={}", record.key(), record.value().getClass());

        try {
            JsonNode docNode;
            Object value = record.value();

            // Detect the type of record value
            if (value instanceof byte[]) {
                log.info("Record value is byte[], parsing to JSON string");
                String jsonString = new String((byte[]) value, StandardCharsets.UTF_8);
                log.info("Raw JSON string: {}", jsonString);
                docNode = mapper.readTree(jsonString);
            } else if (value instanceof String) {
                log.info("Record value is String, parsing to JSON");
                log.info("Raw string: {}", value);
                docNode = mapper.readTree((String) value);
            } else if (value instanceof Map) {
                log.info("Record value is Map, converting to JsonNode");
                docNode = mapper.valueToTree(value);
                log.info("Converted Map to JsonNode");
            } else {
                log.info("Unsupported record value type: {}", value.getClass());
                return record;
            }

            // Flatten JSON into JEXL context
            JexlContext context = new MapContext();
            log.info("Flattening JSON into JEXL context for record key={}", record.key());
            flattenJson("", docNode, context);

            // Evaluate JEXL expression
            Boolean result = (Boolean) expression.evaluate(context);
            log.info("JEXL expression evaluated to {} for record key={}", result, record.key());

            if (Boolean.FALSE.equals(result)) {
                log.info("Record filtered out by expression, key={}", record.key());
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

            log.info("Record passed filter, producing new record: key={} value={}", record.key(), mapValue);

            return newRecord;

        } catch (Exception e) {
            log.info("Error applying filter to record key={}", record.key(), e);
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

    // Recursive helper to flatten JSON into JEXL context
    private void flattenJson(String prefix, JsonNode node, JexlContext ctx) {
        if (node.isObject()) {
            if (!prefix.isEmpty()) {
                ctx.set(prefix, true); // object presence
                log.info("Flattened object key={} as present", prefix);
            }
            node.fieldNames().forEachRemaining(fieldName -> {
                String key = prefix.isEmpty() ? fieldName : prefix + "." + fieldName;
                flattenJson(key, node.get(fieldName), ctx);
            });
        } else if (node.isArray()) {
            if (!prefix.isEmpty()) {
                ctx.set(prefix, true); // array presence
                log.info("Flattened array key={} as present", prefix);
            }
            int index = 0;
            for (JsonNode element : node) {
                String key = prefix + "[" + index + "]";
                flattenJson(key, element, ctx);
                index++;
            }
        } else {
            Object value = null;
            if (node.isBoolean()) value = node.booleanValue();
            else if (node.isNumber()) value = node.numberValue();
            else if (node.isTextual()) value = node.textValue();
            else if (node.isNull()) value = null;

            ctx.set(prefix, value);
            log.info("Flattened leaf key={} value={}", prefix, value);
        }
    }
}
