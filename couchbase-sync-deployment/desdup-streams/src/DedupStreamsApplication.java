package com.path.stream.app;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.*;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

/**
 * =============================================================
 *  DedupStreamsApplication
 * =============================================================
 * This is the main entry point for the Kafka Streams Deduplication App.
 * It reads data from input topics, removes duplicates using both
 * a local in-memory cache (Caffeine) and a GlobalKTable for global
 * deduplication across distributed instances.
 */
@SpringBootApplication
@EnableKafkaStreams
public class DedupStreamsApplication {

    private static final Logger log = LoggerFactory.getLogger(DedupStreamsApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DedupStreamsApplication.class, args);
    }

    /**
     * -------------------------------------------------------------
     * Kafka Streams configuration bean
     * -------------------------------------------------------------
     * Defines the Kafka Streams application properties such as:
     * - Application ID
     * - Bootstrap servers
     * - State directory
     * - SerDes configurations
     */
    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dedup-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500L);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // String tmpDir = System.getProperty("java.io.tmpdir");
        // String stateDir = tmpDir.endsWith("/") ? tmpDir + "kafka-streams-dedup" : tmpDir + "/kafka-streams-dedup";
        // props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        log.info("Kafka Streams configuration initialized");
        return new KafkaStreamsConfiguration(props);
    }

    /**
     * -------------------------------------------------------------
     * Deduplication Topology Builder
     * -------------------------------------------------------------
     * Reads topic mappings from a properties file and builds
     * stream topologies dynamically.
     * Each input topic has a corresponding output topic and a
     * global state store for deduplication.
     */
    @Bean
    public KStream<String, String> buildDedupStream(StreamsBuilder builder) throws Exception {
        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");
        log.info("Building deduplication topology from mapping file");

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String outputTopic = mapping.outputTopic;

            String globalStoreName = ("global-" + outputTopic).replaceAll("[^A-Za-z0-9_\\-]", "_");

            // Create GlobalKTable for deduplication across instances
            builder.globalTable(
                    outputTopic,
                    Consumed.with(Serdes.String(), Serdes.String()),
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(globalStoreName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String())
            );

            // Stream from input topic
            KStream<String, String> input = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

            // Deduplication transformation
            KStream<String, String> deduped = input
                    .transformValues(() -> new DedupTransformerWithCache(globalStoreName), Named.as("dedup-" + globalStoreName))
                    .filter((k, v) -> v != null);

            // Write unique records to output topic
            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            log.info("Topology linked: input={} → output={} → store={}", inputTopic, outputTopic, globalStoreName);
        }
        return null;
    }

    /**
     * =============================================================
     * DedupTransformerWithCache
     * =============================================================
     * Custom transformer that:
     * 1. Uses an in-memory cache (Caffeine) for short-term deduplication.
     * 2. Uses a GlobalKTable for distributed deduplication.
     */
    public static class DedupTransformerWithCache implements ValueTransformerWithKey<String, String, String> {

        private static final Logger logger = LoggerFactory.getLogger(DedupTransformerWithCache.class);
        private final String globalStoreName;
        private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalStore;
        private ProcessorContext context;
        private Cache<String, String> localCache;

        private final long cacheTtlSeconds = 10;

        public DedupTransformerWithCache(String globalStoreName) {
            this.globalStoreName = globalStoreName;
        }

        /**
         * -------------------------------------------------------------
         * Initialize transformer context and cache
         * -------------------------------------------------------------
         * Loads the global state store and initializes Caffeine cache.
         */
        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.localCache = Caffeine.newBuilder()
                    .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
                    .maximumSize(100_000)
                    .build();

            logger.info("Initializing DedupTransformerWithCache for store='{}'", globalStoreName);

            try {
                this.globalStore =
                        (ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>) context.getStateStore(globalStoreName);
                if (globalStore != null) {
                    logger.info("Global store '{}' loaded successfully", globalStoreName);
                }
            } catch (Exception e) {
                logger.warn("Global store '{}' not available at init: {}", globalStoreName, e.getMessage());
            }

            // Re-check store availability periodically
            context.schedule(Duration.ofSeconds(3), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                if (globalStore == null) {
                    try {
                        this.globalStore =
                                (ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>) context.getStateStore(globalStoreName);
                        if (globalStore != null) {
                            logger.info("Global store '{}' became available at runtime.", globalStoreName);
                        }
                    } catch (Exception ignored) {}
                }
            });
        }

        /**
         * -------------------------------------------------------------
         * Core transformation logic
         * -------------------------------------------------------------
         * Checks:
         * 1. Local cache (Caffeine)
         * 2. Global store (GlobalKTable)
         * If duplicate → skips record, else → returns value.
         */
        @Override
        public String transform(String key, String value) {
            if (key == null || value == null) {
                return null;
            }

            String newHash = computeHash(value);

            // Step 1: Check in-memory cache
            String cached = localCache.getIfPresent(key);
            if (cached != null && cached.equals(newHash)) {
                logger.info("[CACHE HIT] Duplicate detected for key={}", key);
                return null;
            }

            // Step 2: Check global store
            if (globalStore != null) {
                try {
                    ValueAndTimestamp<String> storedRecord = globalStore.get(key);
                    if (storedRecord != null) {
                        String storedHash = computeHash(storedRecord.value());
                        if (storedHash.equals(newHash)) {
                            logger.info("[GLOBAL HIT] Duplicate found in GlobalKTable for key={}", key);
                            localCache.put(key, newHash);
                            return null;
                        }
                    }
                } catch (Exception e) {
                    logger.error("Error reading global store '{}' for key={}: {}", globalStoreName, key, e.getMessage());
                }
            }

            // Step 3: Accept new record
            localCache.put(key, newHash);
            logger.info("[ACCEPT] New record accepted for key={}", key);
            return value;
        }

        @Override
        public void close() {
            logger.info("Closing DedupTransformerWithCache for store='{}'", globalStoreName);
        }
    }

    /**
     * -------------------------------------------------------------
     * computeHash()
     * -------------------------------------------------------------
     * Generates a SHA-256 hash of a JSON string for duplicate comparison.
     */
    private static String computeHash(String json) {
        if (json == null) return "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            log.error("Error computing hash: {}", e.getMessage());
            return "";
        }
    }
}
