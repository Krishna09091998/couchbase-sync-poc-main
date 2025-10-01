package com.couchbase.connect.kafka.example;

import com.couchbase.connect.kafka.filter.Filter;
import org.apache.kafka.connect.source.SourceRecord;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * A custom Couchbase Kafka Source Connector event filter that suppresses duplicate events
 * by comparing a version field (e.g., mutationCounter) using RocksDB as a local state store.
 */
public class VersionFilter implements Filter {
    private static final Logger LOGGER = LoggerFactory.getLogger(VersionFilter.class);
    private RocksDB db;
    private final String dbPath = "/tmp/version-filter-db";

    @Override
    public void configure(Map<String, String> config) {
        try {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, dbPath);
            LOGGER.info("RocksDB initialized at {}", dbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
        }
    }

    @Override
    public boolean pass(SourceRecord record) {
        Object valueObj = record.value();
        LOGGER.info("Received SourceRecord with value type: {}", valueObj.getClass().getName());
        LOGGER.info("Record value: {}", valueObj);

        if (!(valueObj instanceof Map)) {
            LOGGER.warn("Unexpected value type: {}", valueObj.getClass().getName());
            return true; // Allow non-map records through
        }

        Map<String, Object> document = (Map<String, Object>) valueObj;
        String docId = (String) document.get("id");
        Object versionObj = document.get("mutationCounter");

        if (docId == null || versionObj == null) {
            LOGGER.warn("Missing 'id' or 'mutationCounter' in document: {}", document);
            return true; // Allow through if missing fields
        }

        int incomingVersion;
        try {
            incomingVersion = Integer.parseInt(versionObj.toString());
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid mutationCounter format for docId={}: {}", docId, versionObj);
            return true;
        }

        try {
            byte[] storedBytes = db.get(docId.getBytes());
            int storedVersion = storedBytes == null ? -1 : Integer.parseInt(new String(storedBytes));

            if (incomingVersion > storedVersion) {
                db.put(docId.getBytes(), String.valueOf(incomingVersion).getBytes());
                LOGGER.info("Forwarding docId={} with new version={}", docId, incomingVersion);
                return true;
            } else {
                LOGGER.info("Filtered out docId={} with unchanged version={}", docId, incomingVersion);
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Error accessing RocksDB for docId={}", docId, e);
            return true; // Fail open
        }
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            LOGGER.info("RocksDB closed");
        }
    }
}
