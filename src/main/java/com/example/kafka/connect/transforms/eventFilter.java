package com.example.kafka.connect.transforms;

import com.couchbase.connect.kafka.filter.EventFilter;
import com.couchbase.connect.kafka.handler.source.SourceHandlerParams;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class SuppressMetadataChangesFilter implements EventFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SuppressMetadataChangesFilter.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private RocksDB db;
    private Options options;

    // Directory for RocksDB storage (can be configured)
    private String dbPath = "/tmp/kafka-rocksdb-version-filter";

    static {
        RocksDB.loadLibrary();
    }

    @Override
    public void init(Map<String, String> config) {
        if (config.containsKey("rocksdb.path")) {
            dbPath = config.get("rocksdb.path");
        }
        try {
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, new File(dbPath).getAbsolutePath());
            LOGGER.info("SuppressMetadataChangesFilter initialized at path={}", dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to initialize RocksDB at " + dbPath, e);
        }
    }

    @Override
    public boolean pass(SourceHandlerParams params) {
        if (!params.isMutation()) {
            return true; // allow deletes/expirations
        }

        String docKey = params.key().toString();
        Object value = params.value();

        try {
            // Parse JSON body
            JsonNode root;
            if (value instanceof byte[]) {
                root = MAPPER.readTree((byte[]) value);
            } else {
                root = MAPPER.readTree(value.toString());
            }

            if (!root.has("_version")) {
                LOGGER.info("Doc {} has no _version field, allowing event.", docKey);
                return true;
            }

            long currentVersion = root.get("_version").asLong();

            // Check previous version from RocksDB
            byte[] lastBytes = db.get(docKey.getBytes(StandardCharsets.UTF_8));
            if (lastBytes != null) {
                long lastSeen = Long.parseLong(new String(lastBytes, StandardCharsets.UTF_8));
                if (lastSeen == currentVersion) {
                    LOGGER.info("Suppressing duplicate for key={}, version={}", docKey, currentVersion);
                    return false; // suppress event
                }
            }

            // Update RocksDB with current version
            db.put(docKey.getBytes(StandardCharsets.UTF_8),
                   String.valueOf(currentVersion).getBytes(StandardCharsets.UTF_8));

            LOGGER.info("Allowing event for key={}, version={}", docKey, currentVersion);
            return true;

        } catch (Exception e) {
            LOGGER.error("Error parsing doc for key=" + docKey, e);
            return true; // fail-safe: allow event
        }
    }

    @Override
    public void close() {
        try {
            if (db != null) db.close();
            if (options != null) options.close();
            LOGGER.info("SuppressMetadataChangesFilter closed.");
        } catch (Exception e) {
            LOGGER.error("Error closing RocksDB", e);
        }
    }
}
