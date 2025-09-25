package com.example.kafka.connect.transforms.procedurebillingcode;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;

public class GenerateDocumentId<R extends ConnectRecord<R>> implements Transformation<R> {

    private interface ConfigName {
        String HASH_FIELDS = "hash.fields";
        String TARGET_FIELD = "hash.target.field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
        .define(ConfigName.HASH_FIELDS, ConfigDef.Type.STRING,
                ConfigDef.Importance.HIGH, "Comma-separated list of fields to hash")
        .define(ConfigName.TARGET_FIELD,
                ConfigDef.Type.STRING, "_id", 
                ConfigDef.Importance.HIGH, "Field name to store the generated hash ID");

    private static final String PURPOSE = "adding hashing to fields";
    private List<String> fieldsToHash;
    private String targetField;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldsToHash = Arrays.asList(config.getString(ConfigName.HASH_FIELDS).split(","));
        targetField = config.getString(ConfigName.TARGET_FIELD);
    }

    @Override
    public R apply(R record) {
        // return null when value is null
        if (operatingValue(record) == null) {
            return record;
        } else {
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final Map<String, Object> updatedValue = new HashMap<>(value);

            // Concatenate selected fields with "::" using streams
            String concatenated = fieldsToHash.stream()
                .map(value::get)
                .filter(v -> v != null)
                .map(Object::toString)
                .collect(Collectors.joining("::"));

            // return the old records when the hash fields are missing in message
            if (concatenated.isEmpty()) {
                return record;
            }

            // Generate hash ID
            String hashId = sha256(concatenated);

            // Add hash ID to the record
            updatedValue.put(targetField, hashId);

            return newRecord(record, null, updatedValue);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    protected Object operatingValue(R record) {
        return record.value();
    }

    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                updatedSchema, updatedValue, record.timestamp());
    }

    private static final ThreadLocal<MessageDigest> DIGEST_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("SHA-256 algorithm not available", e);
        }
    });

    @Override
    public void close() {
        // No resources to clean up
    }

    // hashing the input string
    private String sha256(String input) {
        // using MessageDigest instance to compute SHA-256 Hash
        MessageDigest digest = DIGEST_THREAD_LOCAL.get();
        digest.reset();
        byte[] hash = digest.digest(input.getBytes());

        // converting the hash bytes to a hexadecimal string
        StringBuilder hexString = new StringBuilder();
        for (byte b : hash) {
            hexString.append(String.format("%02x", b));
        }

        // format the hexa string to uuid format 8-4-4-4-12
        String hex = hexString.toString().substring(0, 32);
        return String.format("%s-%s-%s-%s-%s", 
            hex.substring(0, 8), 
            hex.substring(8, 12), 
            hex.substring(12, 16),
            hex.substring(16, 20), 
            hex.substring(20, 32));
    }
}
