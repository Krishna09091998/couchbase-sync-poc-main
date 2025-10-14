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

import java.io.File;
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

        initRocksDB(rocksPath);
    }

    private void initRocksDB(String path) {
        try {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);

            File dbDir = new File(path);
            File lockFile = new File(dbDir, "LOCK");

            // If LOCK exists, attempt recovery
            if (lockFile.exists()) {
                log.warn("RocksDB LOCK file exists. Attempting to recover: {}", lockFile.getAbsolutePath());
                if (db != null) {
                    try {
                        db.close(); // close any previous handle
                        log.info("Closed existing RocksDB instance for path {}", path);
                    } catch (Exception e) {
                        log.error("Error closing previous RocksDB instance", e);
                    }
                    db = null;
                }
                // Delete the LOCK file
                boolean deleted = lockFile.delete();
                log.info("Deleted stale LOCK file: {} success={}", lockFile.getAbsolutePath(), deleted);
            }

            // Open RocksDB
            db = RocksDB.open(options, path);
            digest = MessageDigest.getInstance("SHA-256");
            log.info("RocksDB initialized at {}", path);

        } catch (RocksDBException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to initialize RocksDB", e);
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
