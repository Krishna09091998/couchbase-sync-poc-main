package com.path.stream.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
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
        // Make updates visible faster (reduce buffering). Use with caution for throughput.
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500L);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        return new KafkaStreamsConfiguration(props);
    }

    private KafkaStreams kafkaStreams;

    @EventListener
    public void onKafkaStreamsReady(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    /**
     * Build topology. Use void since we create multiple streams inside the loop.
     */
    @Bean
    public void buildDedupStream(StreamsBuilder builder) {
        // Your mapper - assumed to provide inputTopic -> mapping (dedupTopic, outputTopic)
        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String dedupTopic = mapping.dedupTopic;
            String outputTopic = mapping.outputTopic;

            // Create a KTable (partition-local, synchronous)
            KTable<String, String> dedupTable = builder.table(
                dedupTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(dedupTopic)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withLoggingEnabled(new HashMap<>()) // enable changelog (default settings)
            );

            // Stream from input topic
            KStream<String, String> stream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
            );

            // Dedup: leftJoin stream with the partition-local KTable
            KStream<String, String> deduped = stream.leftJoin(
                dedupTable,
                (key, value) -> key, // use same key for lookup
                (value, existingHash) -> {
                    String newHash = computeHash(value);
                    if (existingHash == null || !existingHash.equals(newHash)) {
                        return value; // new/changed -> pass through
                    } else {
                        return null; // duplicate -> drop
                    }
                }
            ).filter((k, v) -> v != null); // remove nulls (duplicates)

            // Publish deduplicated records to output topic
            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            // Update the dedup hash topic with new hash values (key stays same)
            deduped.mapValues(DedupStreamsApplication::computeHash)
                   .to(dedupTopic, Produced.with(Serdes.String(), Serdes.String()));
        }
    }

    private static String computeHash(String json) {
        if (json == null) return "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            // log error in real code
            return "";
        }
    }
}
