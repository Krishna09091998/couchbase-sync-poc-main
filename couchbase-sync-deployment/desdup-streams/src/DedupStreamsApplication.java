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

@SpringBootApplication
@EnableKafkaStreams
public class DedupStreamsApplication {

    private static final Logger log = LoggerFactory.getLogger(DedupStreamsApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DedupStreamsApplication.class, args);
    }

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

        String tmpDir = System.getProperty("java.io.tmpdir");
        String stateDir = tmpDir.endsWith("/") ? tmpDir + "kafka-streams-dedup" : tmpDir + "/kafka-streams-dedup";
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

        log.info("Kafka Streams config: state.dir={}", stateDir);
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> buildDedupStream(StreamsBuilder builder) throws Exception {

        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String outputTopic = mapping.outputTopic;

            String rawStoreName = "global-" + outputTopic;
            String globalStoreName = rawStoreName.replaceAll("[^A-Za-z0-9_\\-]", "_");

            log.info("Wiring mapping: inputTopic={} outputTopic={} globalStore={}", inputTopic, outputTopic, globalStoreName);

            // GlobalKTable subscribed to output topic (dedup reference)
            builder.globalTable(
                    outputTopic,
                    Consumed.with(Serdes.String(), Serdes.String()),
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(globalStoreName)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String())
            );

            KStream<String, String> input = builder.stream(
                    inputTopic,
                    Consumed.with(Serdes.String(), Serdes.String())
            );

            KStream<String, String> deduped = input.transformValues(
                    () -> new DedupTransformerWithCache(globalStoreName),
                    Named.as("dedup-" + globalStoreName)
            ).filter((k, v) -> v != null);

            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        return null;
    }

    public static class DedupTransformerWithCache implements ValueTransformerWithKey<String, String, String> {

        private static final Logger logger = LoggerFactory.getLogger(DedupTransformerWithCache.class);

        private final String globalStoreName;
        private ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> globalStore;
        private ProcessorContext context;
        private Cache<String, String> localCache;

        private final long cacheTtlSeconds = 10;
        private final long cachePrintIntervalSeconds = 30;

        public DedupTransformerWithCache(String globalStoreName) {
            this.globalStoreName = globalStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.localCache = Caffeine.newBuilder()
                    .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
                    .maximumSize(100_000)
                    .build();

            logger.info("Initializing DedupTransformerWithCache for store='{}'", globalStoreName);

            try {
                ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store =
                        (ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>) context.getStateStore(globalStoreName);
                if (store != null) {
                    this.globalStore = store;
                    logger.info("Global store '{}' obtained at init()", globalStoreName);
                }
            } catch (Exception e) {
                logger.warn("Global store '{}' not available yet at init(): {}", globalStoreName, e.getMessage());
            }

            context.schedule(Duration.ofSeconds(3), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                if (globalStore == null) {
                    try {
                        ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>> store =
                                (ReadOnlyKeyValueStore<String, ValueAndTimestamp<String>>) context.getStateStore(globalStoreName);
                        if (store != null) {
                            this.globalStore = store;
                            logger.info("Global store '{}' became available at runtime.", globalStoreName);
                        }
                    } catch (Exception ignored) {}
                }

                try {
                    long cacheSize = localCache.estimatedSize();
                    logger.debug("Cache (store={}) estimated size = {}", globalStoreName, cacheSize);
                } catch (Exception ignored) {}
            });
        }

        @Override
        public String transform(String key, String value) {
            if (key == null || value == null) {
                logger.debug("Skipping null key/value for key={}", key);
                return null;
            }

            String newHash = computeHash(value);

            // 1️⃣ Check in-memory cache
            String cached = localCache.getIfPresent(key);
            if (cached != null && cached.equals(newHash)) {
                logger.info("[CACHE HIT] Duplicate detected in cache for key={}", key);
                return null;
            }

            // 2️⃣ Check global store
            if (globalStore != null) {
                try {
                    ValueAndTimestamp<String> storedRecord = globalStore.get(key);
                    if (storedRecord != null) {
                        String storedValue = storedRecord.value();
                        String storedHash = computeHash(storedValue);
                        if (storedHash.equals(newHash)) {
                            logger.info("[GLOBAL HIT] Duplicate detected in GlobalKTable for key={}", key);
                            localCache.put(key, newHash);
                            return null;
                        }
                    }
                } catch (Exception e) {
                    logger.warn("Error reading global store '{}' for key={}: {}", globalStoreName, key, e.getMessage());
                }
            } else {
                logger.debug("[GLOBAL CHECK SKIPPED] Global store '{}' not ready for key={}", globalStoreName, key);
            }

            // 3️⃣ Accept and cache
            localCache.put(key, newHash);
            logger.info("[ACCEPT] key={} accepted and cached (hash={})", key, newHash);
            return value;
        }

        @Override
        public void close() {
            logger.info("Closing DedupTransformerWithCache for store='{}'", globalStoreName);
        }
    }

    private static String computeHash(String json) {
        if (json == null) return "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            return "";
        }
    }
}
