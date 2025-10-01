package com.couchbase.connect.kafka.example;

import java.util.Map;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import com.couchbase.client.java.json.JsonObject;

/**
 * CustomFilter.java (modified)
 *
 * Filters Couchbase change events by maintaining a key->version
 * map in RocksDB. Events with the same key and version as previously
 * seen are ignored.
 */
public class CustomFilter {

    private RocksDB db;
    private Options options;
    private String versionField = "version"; // default version field

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Initialize RocksDB store and read configuration.
     *
     * @param props connector config properties
     */
    public void init(Map<String, String> props) {
        if (props.containsKey("version.field")) {
            versionField = props.get("version.field");
        }

        try {
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, "rocksdb_store"); // folder for RocksDB files
        } catch (RocksDBException e) {
            throw new RuntimeException("Error initializing RocksDB", e);
        }
    }

    /**
     * Filter method called for each document change.
     *
     * @param document the Couchbase document as an Object
     * @return true to process, false to ignore
     */
    public boolean filter(Object document) {
        if (document == null) {
            return true; // allow null events
        }

        // Attempt to cast document to JsonObject if possible
        JsonObject json;
        if (document instanceof JsonObject) {
            json = (JsonObject) document;
        } else {
            // If it’s not a JsonObject, cannot filter by version
            return true;
        }

        if (!json.containsKey(versionField)) {
            return true; // allow if no version info
        }

        String key = json.getString("id"); // adjust if key is stored elsewhere
        int version = json.getInt(versionField);

        try {
            byte[] existing = db.get(key.getBytes());

            if (existing != null) {
                int storedVersion = Integer.parseInt(new String(existing));
                if (storedVersion >= version) {
                    // Already processed → ignore
                    return false;
                }
            }

            // Update RocksDB with latest version
            db.put(key.getBytes(), String.valueOf(version).getBytes());

        } catch (RocksDBException e) {
            throw new RuntimeException("Error accessing RocksDB", e);
        }

        return true; // process event
    }

    /**
     * Close RocksDB on connector shutdown.
     */
    public void close() {
        if (db != null) db.close();
        if (options != null) options.close();
    }
}
