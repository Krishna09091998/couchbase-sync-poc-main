package com.path.stream.app;

import com.example.DedupTopicMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
@EnableKafkaStreams
public class DedupStreamsApplication {

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
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams-dedup");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> buildDedupStream(StreamsBuilder builder) throws Exception {

        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String outputTopic = mapping.outputTopic;

            // GlobalKTable subscribed to output topic (dedup reference)
            GlobalKTable<String, String> globalTable = builder.globalTable(
                    outputTopic,
                    Consumed.with(Serdes.String(), Serdes.String()),
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("global-" + outputTopic)
                            .withKeySerde(Serdes.String())
                            .withValueSerde(Serdes.String())
            );

            KStream<String, String> input = builder.stream(
                    inputTopic,
                    Consumed.with(Serdes.String(), Serdes.String())
            );

            // Transformer with cache + global store lookup
            KStream<String, String> deduped = input.transformValues(
                    () -> new DedupTransformerWithCache("global-" + outputTopic),
                    Named.as("dedup-" + outputTopic)
            ).filter((k, v) -> v != null);

            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        return null;
    }

    public static class DedupTransformerWithCache implements ValueTransformerWithKey<String, String, String> {

        private final String globalStoreName;
        private ReadOnlyKeyValueStore<String, String> globalStore;
        private ProcessorContext context;
        private Cache<String, String> localCache;

        public DedupTransformerWithCache(String globalStoreName) {
            this.globalStoreName = globalStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.localCache = Caffeine.newBuilder()
                    .expireAfterWrite(10, TimeUnit.SECONDS)
                    .maximumSize(100_000)
                    .build();

            // We’ll fetch the GlobalKTable store at runtime
            this.context.schedule(Duration.ofSeconds(5), PunctuationType.WALL_CLOCK_TIME, timestamp -> {
                if (globalStore == null) {
                    try {
                        globalStore = ((KafkaStreams) context.appConfigs().get(StreamsConfig.APPLICATION_ID_CONFIG))
                                .store(StoreQueryParameters.fromNameAndType(globalStoreName, QueryableStoreTypes.keyValueStore()));
                    } catch (Exception ignored) {}
                }
            });
        }

        @Override
        public String transform(String key, String value) {
            if (key == null || value == null) return null;

            String hash = computeHash(value);

            // 1️⃣ Check in-memory cache
            String cached = localCache.getIfPresent(key);
            if (hash.equals(cached)) {
                System.out.println("⏩ Dropped duplicate from in-memory cache: " + key);
                return null;
            }

            // 2️⃣ Check GlobalKTable (if available)
            if (globalStore != null) {
                String stored = globalStore.get(key);
                if (hash.equals(stored)) {
                    System.out.println("🧩 Dropped duplicate from GlobalKTable: " + key);
                    return null;
                }
            }

            // 3️⃣ Accept and cache
            localCache.put(key, hash);
            return value;
        }

        @Override
        public void close() {}
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
