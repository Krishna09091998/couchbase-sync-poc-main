package com.path.custom.kafka.connect.transforms;

import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.JsonNode;
import com.couchbase.client.core.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;
import org.apache.commons.jexl3.*;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;

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
            log.debug("Record value is null, skipping key={}", record.key());
            return null;
        }

        try {
            Object value = record.value();
            JsonNode docNode;

            // Parse record value
            if (value instanceof byte[]) {
                String jsonString = new String((byte[]) value, StandardCharsets.UTF_8);
                docNode = mapper.readTree(jsonString);
            } else if (value instanceof String) {
                docNode = mapper.readTree((String) value);
            } else if (value instanceof Map) {
                docNode = mapper.valueToTree(value);
            } else {
                log.warn("Unsupported record value type: {}", value.getClass());
                return record;
            }

            // ✅ Use JsonFlattener to flatten the JSON
            String jsonString = mapper.writeValueAsString(docNode);
            Map<String, Object> flattenedMap = JsonFlattener.flattenAsMap(jsonString);

            // Evaluate JEXL expression using flattened map
            JexlContext context = new MapContext(flattenedMap);
            Boolean result = (Boolean) expression.evaluate(context);

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

            log.debug("Record passed filter, producing new record: key={}", record.key());
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
}
