package com.path.stream.app;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsConfig;
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
        // recommend earliest for dev/test when you want to pick up prior records if no committed offsets
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // state dir: include app id so different apps don't collide; can be overridden by env/config
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
            // normalize store name (avoid characters that may confuse store naming)
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

            // Transformer with cache + global store lookup
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
        private ReadOnlyKeyValueStore<String, String> globalStore;
        private ProcessorContext context;
        private Cache<String, String> localCache;

        // cache configs - tune as needed
        private final long cacheTtlSeconds = 10;
        private final long cachePrintIntervalSeconds = 30; // show cache size/metrics periodically

        public DedupTransformerWithCache(String globalStoreName) {
            this.globalStoreName = globalStoreName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            this.context = context;
            this.localCache = Caffeine.newBuilder()
                    .expireAfterWrite(cacheTtlSeconds, TimeUnit.SECONDS)
                    .maximumSize(100_000)
                    .build();

            logger.info("DedupTransformerWithCache.init() called for store='{}' - starting retry to obtain store", globalStoreName);

            // Try to get the store immediately; if not available, schedule repeated tries.
            try {
                StateStore s = context.getStateStore(globalStoreName);
                if (s != null && s instanceof ReadOnlyKeyValueStore) {
                    this.globalStore = (ReadOnlyKeyValueStore<String, String>) s;
                    logger.info("Global store '{}' obtained synchronously at init()", globalStoreName);
                } else {
                    logger.info("Global store '{}' not available at init(); scheduling retry", globalStoreName);
                }
            } catch (Exception e) {
                logger.warn("Exception while trying to get state store at init(): {}", e.getMessage());
            }

            // schedule a periodic task to try to acquire the store and to log cache size
            context.schedule(Duration.ofSeconds(2), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                if (globalStore == null) {
                    try {
                        StateStore s = context.getStateStore(globalStoreName);
                        if (s != null && s instanceof ReadOnlyKeyValueStore) {
                            this.globalStore = (ReadOnlyKeyValueStore<String, String>) s;
                            logger.info("Global store '{}' is now available (scheduled retry).", globalStoreName);
                        }
                    } catch (Exception ex) {
                        // ignore; next punctuation will retry
                    }
                }
                // periodic cache metric/log
                try {
                    long size = localCache.estimatedSize();
                    logger.debug("Cache (store={}) estimated size = {}", globalStoreName, size);
                } catch (Exception ex) {
                    // ignore
                }
            });
        }

        @Override
        public String transform(String key, String value) {
            if (key == null || value == null) {
                logger.debug("Skipping null key/value (key={}, value==null?)", key);
                return null;
            }

            final String newHash = computeHash(value);

            // 1) Fast local cache check
            try {
                String cached = localCache.getIfPresent(key);
                if (cached != null) {
                    if (cached.equals(newHash)) {
                        logger.info("[CACHE HIT] key='{}' duplicate detected in local cache", key);
                        return null;
                    } else {
                        logger.debug("[CACHE STALE] key='{}' cachedHash!=newHash (cached={}, new={})", key, cached, newHash);
                    }
                } else {
                    logger.debug("[CACHE MISS] key='{}'", key);
                }
            } catch (Exception ex) {
                logger.warn("Error reading local cache for key='{}': {}", key, ex.getMessage());
            }

            // 2) Global store check (authoritative; may be null until restored)
            if (globalStore == null) {
                logger.debug("[GLOBAL CHECK SKIPPED] global store '{}' not yet available for key='{}'", globalStoreName, key);
            } else {
                try {
                    String stored = globalStore.get(key);
                    if (stored != null) {
                        String storedHash = stored; // assuming stored value is already the hash or the JSON — adapt if you store JSON
                        // If stored value is JSON and you store hash in store, compare hashes accordingly.
                        // Here we assume value stored in global table is the same JSON and we compute hash for comparison.
                        String storedHashComputed = computeHash(stored);
                        if (storedHashComputed.equals(newHash)) {
                            logger.info("[GLOBAL HIT] key='{}' duplicate detected in global store", key);
                            // also insert into local cache (so subsequent duplicates are caught quickly)
                            localCache.put(key, newHash);
                            return null;
                        } else {
                            logger.debug("[GLOBAL MISS] key='{}' stored hash != new hash", key);
                        }
                    } else {
                        logger.debug("[GLOBAL MISS] key='{}' not present in global store", key);
                    }
                } catch (Exception ex) {
                    logger.warn("Error reading global store '{}' for key='{}': {}", globalStoreName, key, ex.getMessage());
                }
            }

            // 3) Accept and cache
            localCache.put(key, newHash);
            logger.info("[ACCEPT] key='{}' accepted and cached (hash={})", key, newHash);
            return value;
        }

        @Override
        public void close() {
            // nothing to close; cache will be GC'd on shutdown
            logger.info("DedupTransformerWithCache.close() called for store='{}'", globalStoreName);
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
