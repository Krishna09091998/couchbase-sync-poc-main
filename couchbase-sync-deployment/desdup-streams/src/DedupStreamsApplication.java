package com.path.stream.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 500L); // faster commit
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0); // disable cache for instant state visibility
        return new KafkaStreamsConfiguration(props);
    }

    private KafkaStreams kafkaStreams;

    @EventListener
    public void onKafkaStreamsReady(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
        log.info("Kafka Streams instance ready");
    }

    @Bean
    public KStream<String, String> buildDedupStream(StreamsBuilder builder) {
        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");
        KStream<String, String> lastStream = null;

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String dedupTopic = mapping.dedupTopic;
            String outputTopic = mapping.outputTopic;

            log.info("=== Processing input topic: {} ===", inputTopic);

            // Create partition-local KTable for deduplication
            KTable<String, String> dedupTable = builder.table(
                dedupTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(dedupTopic)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
                    .withLoggingEnabled(new HashMap<>())
            );
            log.info("Dedup KTable created for topic: {}", dedupTopic);

            // Stream from input topic
            KStream<String, String> stream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
            );
            log.info("Stream created for input topic: {}", inputTopic);

            // Deduplication logic
            KStream<String, String> deduped = stream.leftJoin(
                dedupTable,
                (value, existingHash) -> {
                    String newHash = computeHash(value);
                    if (existingHash == null) {
                        log.info("NEW RECORD key={} hash={}", value, newHash);
                    } else if (!existingHash.equals(newHash)) {
                        log.info("UPDATED RECORD key={} oldHash={} newHash={}", value, existingHash, newHash);
                    } else {
                        log.info("DUPLICATE RECORD key={} hash={}", value, newHash);
                    }

                    return (existingHash == null || !existingHash.equals(newHash)) ? value : null;
                }
            ).filter((k, v) -> v != null);

            // Publish deduplicated records to output topic
            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));
            log.info("Deduped records published to output topic: {}", outputTopic);

            // Update dedup hash topic
            deduped.mapValues(DedupStreamsApplication::computeHash)
                   .to(dedupTopic, Produced.with(Serdes.String(), Serdes.String()));
            log.info("Dedup hash updated to topic: {}", dedupTopic);

            lastStream = deduped;
            log.info("=== Completed processing for input topic: {} ===", inputTopic);
        }

        return lastStream;
    }

    private static String computeHash(String json) {
        if (json == null) return "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            log.error("Error computing hash", e);
            return "";
        }
    }
}
