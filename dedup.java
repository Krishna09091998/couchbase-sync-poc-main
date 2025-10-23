package com.hwow.streams.apps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Properties;

public class EncounterDeduplicationApp {
    private static final Logger logger = LoggerFactory.getLogger(EncounterDeduplicationApp.class);
    private static ReadOnlyKeyValueStore<String, String> dedupStore;

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "encounter-dedup-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        // Load dedup store from compacted topic
        builder.globalTable("path.treatment.sql.data.dedup-store",
            Consumed.with(Serdes.String(), Serdes.String()),
            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("dedup-store-state").withLoggingDisabled());

        // Read filtered documents from connector topic
        KStream<String, String> rawStream = builder.stream("path.treatment.sql.data",
            Consumed.with(Serdes.String(), Serdes.String()));

        // Filter out unchanged documents
        rawStream.filter((key, value) -> shouldEmit(key, value))
                 .to("path.treatment.sql.data.filtered", Produced.with(Serdes.String(), Serdes.String())); // final output

        // Update dedup store with latest hash
        rawStream.mapValues(EncounterDeduplicationApp::computeHash)
                 .to("path.treatment.sql.data.dedup-store", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static boolean shouldEmit(String key, String newJson) {
        if (dedupStore == null) {
            throw new IllegalStateException("Dedup store not initialized yet");
        }

        String newHash = computeHash(newJson);
        String oldHash = dedupStore.get(key);

        boolean isNew = oldHash == null || !newHash.equals(oldHash);
        if (isNew) {
            logger.info("Emitting updated document: {}", key);
        } else {
            logger.info("Suppressing unchanged document: {}", key);
        }
        return isNew;
    }

    private static String computeHash(String json) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            logger.error("Hashing failed", e);
            return "";
        }
    }
}
