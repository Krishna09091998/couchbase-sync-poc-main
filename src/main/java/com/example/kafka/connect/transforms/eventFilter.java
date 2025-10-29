package com.path.stream.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

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
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500L); // faster commit
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return new KafkaStreamsConfiguration(props);
    }

    private KafkaStreams kafkaStreams;

    @EventListener
    public void onKafkaStreamsReady(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    @Bean
    public KStream<String, String> buildDedupStream(StreamsBuilder builder) {

        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");

        // Build persistent KeyValueStore for dedup
        String storeName = "dedup-store";
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore(storeName),
                        Serdes.String(),
                        Serdes.String()
                ).withLoggingEnabled(new HashMap<>()) // enable changelog
        );

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String outputTopic = mapping.outputTopic;

            KStream<String, String> stream = builder.stream(
                    inputTopic,
                    Consumed.with(Serdes.String(), Serdes.String())
            );

            // Apply stateful transformer for synchronous dedup
            KStream<String, String> deduped = stream.transformValues(
                    () -> new DedupTransformer(storeName),
                    storeName
            ).filter((k, v) -> v != null); // drop duplicates

            // Publish deduplicated records to output topic
            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        return null; // bean return is not used
    }

    // Transformer that performs synchronous deduplication
    public static class DedupTransformer implements ValueTransformerWithKey<String, String, String> {

        private final String storeName;
        private KeyValueStore<String, String> kvStore;

        public DedupTransformer(String storeName) {
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.kvStore = (KeyValueStore<String, String>) context.getStateStore(storeName);
        }

        @Override
        public String transform(String key, String value) {
            if (value == null) return null;

            String newHash = computeHash(value);
            String oldHash = kvStore.get(key);

            if (oldHash == null) {
                kvStore.put(key, newHash);
                System.out.println("Accepting new document '" + key + "'");
                return value;
            }

            if (oldHash.equals(newHash)) {
                System.out.println("Rejecting unchanged document '" + key + "'");
                return null; // duplicate
            } else {
                kvStore.put(key, newHash);
                System.out.println("Accepting updated document '" + key + "'");
                return value;
            }
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
