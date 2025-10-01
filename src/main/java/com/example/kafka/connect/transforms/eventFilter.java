package com.couchbase.connect.kafka.example;

import java.util.Map;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import com.couchbase.client.java.json.JsonObject;

/**
 * VersionDedupFilter.java
 *
 * Final, production-ready filter with RocksDB version deduplication.
 * Prevents duplicate processing of documents based on key + version.
 */
public final class VersionDedupFilter extends CustomFilter {

    private RocksDB db;
    private Options options;
    private String versionField = "version";       // field inside document
    private String rocksDbPath = "rocksdb_store";  // default RocksDB folder

    static {
        RocksDB.loadLibrary();
    }

    /**
     * Initialize filter and RocksDB store.
     */
    @Override
    public void init(Map<String, String> props) {
        super.init(props);

        if (props.containsKey("version.field")) {
            versionField = props.get("version.field");
        }

        if (props.containsKey("rocksdb.path")) {
            rocksDbPath = props.get("rocksdb.path");
        }

        try {
            options = new Options().setCreateIfMissing(true);
            db = RocksDB.open(options, rocksDbPath);
        } catch (RocksDBException e) {
            throw new RuntimeException("Error initializing RocksDB at path: " + rocksDbPath, e);
        }
    }

    /**
     * Filter each Couchbase document.
     * Returns false if the same key+version was already processed.
     */
    @Override
    public boolean filter(Object document) {
        boolean shouldProcess = super.filter(document);
        if (!shouldProcess) return false;

        if (!(document instanceof JsonObject)) return true;

        JsonObject json = (JsonObject) document;
        String key = getKey(json);
        int version = getVersion(json);

        if (key == null) return true;
        if (version < 0) return true;

        try {
            byte[] existing = db.get(key.getBytes());
            if (existing != null) {
                int storedVersion = Integer.parseInt(new String(existing));
                if (storedVersion >= version) return false; // duplicate
            }
            db.put(key.getBytes(), String.valueOf(version).getBytes());
        } catch (RocksDBException e) {
            throw new RuntimeException("Error accessing RocksDB", e);
        }

        return true;
    }

    /**
     * Close RocksDB on connector shutdown.
     */
    @Override
    public void close() {
        super.close();
        if (db != null) db.close();
        if (options != null) options.close();
    }

    // ---------------- Helper methods ----------------

    /** Extract document key (id) from JsonObject */
    private String getKey(JsonObject json) {
        if (!json.containsKey("id")) return null;
        return json.getString("id");
    }

    /** Extract document version from JsonObject */
    private int getVersion(JsonObject json) {
        if (!json.containsKey(versionField)) return -1;
        return json.getInt(versionField);
    }
}
