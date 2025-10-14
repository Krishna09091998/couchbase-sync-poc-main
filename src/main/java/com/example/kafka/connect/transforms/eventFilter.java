package com.example.kafka.connect.transforms;

import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
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
import java.util.concurrent.ConcurrentHashMap;

public class CustomFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(CustomFilter.class);

    private static final String ROCKSDB_PATH_CONFIG = "couchbase.custom.filter.rocksdb.path";
    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ROCKSDB_PATH_CONFIG,
                    ConfigDef.Type.STRING,
                    "/tmp/kafka-rocksdb-version-filter",
                    ConfigDef.Importance.HIGH,
                    "Path to RocksDB directory for storing document hashes");

    private RocksDB db;
    private MessageDigest digest;
    private String dbPath;

    // Static map to track open DBs per path
    private static final ConcurrentHashMap<String, RocksDB> openDBs = new ConcurrentHashMap<>();

    @Override
    public void init(Map<String, String> configProperties) {
        dbPath = configProperties.getOrDefault(ROCKSDB_PATH_CONFIG, "/tmp/kafka-rocksdb-version-filter");

        try {
            RocksDB.loadLibrary();
            Options options = new Options().setCreateIfMissing(true);

            // Close existing DB instance if already open
            if (openDBs.containsKey(dbPath)) {
                try {
                    openDBs.get(dbPath).close();
                    log.info("Closed previous RocksDB instance for path {}", dbPath);
                } catch (Exception e) {
                    log.warn("Error closing previous RocksDB for path {}", dbPath, e);
                }
                openDBs.remove(dbPath);
            }

            db = RocksDB.open(options, dbPath);
            openDBs.put(dbPath, db);

            digest = MessageDigest.getInstance("SHA-256");
            log.info("CustomFilter RocksDB initialized at {}", dbPath);

        } catch (RocksDBException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to initialize CustomFilter RocksDB", e);
        }
    }

    @Override
    public boolean pass(DocumentEvent event) {
        if (!event.isMutation()) {
            log.debug("Skipping non-mutation event {}", event);
            return true; // allow deletes/expirations etc.
        }

        try {
            String docId = event.key();
            byte[] body = event.content();

            // Compute hash of the document body
            byte[] newHash = digest.digest(body);

            // Check previous hash
            byte[] oldHash = db.get(docId.getBytes(StandardCharsets.UTF_8));

            if (oldHash == null) {
                // First time seeing this document
                db.put(docId.getBytes(StandardCharsets.UTF_8), newHash);
                log.info("Accepting new document '{}'", docId);
                return true;
            }

            if (MessageDigest.isEqual(oldHash, newHash)) {
                log.info("Rejecting unchanged document '{}'", docId);
                return false;
            } else {
                db.put(docId.getBytes(StandardCharsets.UTF_8), newHash);
                log.info("Accepting updated document '{}'", docId);
                return true;
            }

        } catch (Exception e) {
            log.error("Error filtering document event {}", event, e);
            return true; // Fail open if anything goes wrong
        }
    }
}
