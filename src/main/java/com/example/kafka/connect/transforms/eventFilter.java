package com.couchbase.connect.kafka.example;

import com.couchbase.client.java.json.JsonObject;
import com.couchbase.connect.kafka.filter.Filter;
import com.couchbase.connect.kafka.handler.source.DocumentEvent;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * CustomFilter for RocksDB version deduplication with detailed logging.
 */
public class CustomFilter implements Filter {
    private static final Logger log = LoggerFactory.getLogger(CustomFilter.class);

    private static final String VERSION_FIELD_PROPERTY = "couchbase.custom.filter.version.field";
    private static final String ROCKSDB_PATH_PROPERTY = "couchbase.custom.filter.rocksdb.path";

    private static final ConfigDef configDef = new ConfigDef()
            .define(VERSION_FIELD_PROPERTY,
                    ConfigDef.Type.STRING,
                    "version",
                    ConfigDef.Importance.HIGH,
                    "Field name containing document version.")
            .define(ROCKSDB_PATH_PROPERTY,
                    ConfigDef.Type.STRING,
                    "rocksdb_store",
                    ConfigDef.Importance.HIGH,
                    "Path for RocksDB storage.");

    private RocksDB db;
    private Options options;
    private String versionField;
    private String rocksDbPath;

    static {
        RocksDB.loadLibrary();
    }

    @Override
    public void init(Map<String, String> configProperties) {
        AbstractConfig config = new AbstractConfig(configDef, configProperties);
        versionField = config.getString(VERSION_FIELD_PROPERTY);
        rocksDbPath = config.getString(ROCKSDB_PATH_PROPERTY);

        log.info("Initializing RocksDB deduplication: versionField='{}', path='{}'", versionField, rocksDbPath);

        try {
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, rocksDbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Failed to open RocksDB at path: " + rocksDbPath, e);
        }
    }

    @Override
    public boolean pass(DocumentEvent event) {
        if (!event.isMutation()) {
            log.debug("Skipping non-mutation event: {}", event);
            return true;
        }

        try {
            JsonObject json = JsonObject.fromJson(event.content());
            log.info("Processing document: id={}, content={}", json.getString("id"), json);

            String key = json.containsKey("id") ? json.getString("id") : null;
            if (key == null) {
                log.warn("Document has no 'id' field, passing through: {}", json);
                return true; // cannot dedup without key
            }

            int version = json.containsKey(versionField) ? json.getInt(versionField) : -1;
            if (version < 0) {
                log.warn("Document has no '{}' field or invalid version, passing through: {}", versionField, json);
                return true; // skip if no version
            }

            // Ignore Couchbase metadata keys
            if (key.startsWith("_") || key.equals("$document")) {
                log.debug("Ignoring internal metadata key: {}", key);
                return true;
            }

            byte[] existing = db.get(key.getBytes());
            if (existing != null) {
                int storedVersion = Integer.parseInt(new String(existing));
                if (storedVersion >= version) {
                    log.info("Skipping duplicate document: id={}, version={}, storedVersion={}", key, version, storedVersion);
                    return false; // duplicate, ignore event
                }
            }

            db.put(key.getBytes(), String.valueOf(version).getBytes());
            log.info("Accepted document: id={}, version={}", key, version);

        } catch (Exception e) {
            log.error("Error processing document for RocksDB deduplication: {}", event, e);
        }

        return true;
    }

    @Override
    public void close() {
        try {
            if (db != null) db.close();
            if (options != null) options.close();
            log.info("Closed RocksDB for CustomFilter");
        } catch (Exception e) {
            log.error("Error closing RocksDB", e);
        }
    }
}
