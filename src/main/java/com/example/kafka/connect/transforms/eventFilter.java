package com.couchbase.connect.kafka.example;

import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

public class CustomFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(CustomFilter.class);

    private static final String ROCKSDB_PATH = "couchbase.custom.filter.rocksdb.path";

    private static final ConfigDef configDef = new ConfigDef()
            .define(ROCKSDB_PATH,
                    ConfigDef.Type.STRING,
                    "/tmp/couchbase-filter-rocksdb",
                    ConfigDef.Importance.HIGH,
                    "Path to RocksDB directory for storing doc hashes");

    private RocksDB db;
    private MessageDigest digest;

    @Override
    public void init(Map<String, String> configProperties) {
        AbstractConfig config = new AbstractConfig(configDef, configProperties);
        String rocksPath = config.getString(ROCKSDB_PATH);

        try {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, rocksPath);
            digest = MessageDigest.getInstance("SHA-256");  // or "MD5"/"MurmurHash"
            log.info("CustomFilter initialized with RocksDB at {}", rocksPath);
        } catch (RocksDBException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to initialize CustomFilter", e);
        }
    }

    @Override
    public boolean pass(DocumentEvent event) {
        if (!event.isMutation()) {
            log.debug("Skipping non-mutation event {}", event);
            return false;
        }

        try {
            String docId = event.key(); // Couchbase doc ID
            byte[] body = event.content();

            // Compute hash of body
            byte[] newHash = digest.digest(body);

            // Lookup old hash
            byte[] oldHash = db.get(docId.getBytes(StandardCharsets.UTF_8));

            if (oldHash == null) {
                // First time seeing this doc
                db.put(docId.getBytes(StandardCharsets.UTF_8), newHash);
                log.info("Accepting new document '{}'", docId);
                return true;
            }

            if (MessageDigest.isEqual(oldHash, newHash)) {
                // Duplicate, skip
                log.info("Rejecting unchanged document '{}'", docId);
                return false;
            } else {
                // Changed, update hash
                db.put(docId.getBytes(StandardCharsets.UTF_8), newHash);
                log.info("Accepting updated document '{}'", docId);
                return true;
            }

        } catch (Exception e) {
            log.error("Error filtering event {}", event, e);
            return true; // Fail open
        }
    }
}
