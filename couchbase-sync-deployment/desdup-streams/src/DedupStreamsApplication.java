package com.path.stream.app;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Materialized;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.context.event.EventListener;

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
        return new KafkaStreamsConfiguration(props);
    }

    private KafkaStreams kafkaStreams;

    @EventListener
    public void onKafkaStreamsReady(KafkaStreams kafkaStreams) {
        this.kafkaStreams = kafkaStreams;
    }

    private final Map<String, ReadOnlyKeyValueStore<String, String>> storeCache = new HashMap<>();

    @Bean
    public KStream<String, String> buildDedupStream(StreamsBuilder builder) {
        DedupTopicMapper mapper = new DedupTopicMapper("dedup-mapping.properties");

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String dedupTopic = mapping.dedupTopic;
            String outputTopic = mapping.outputTopic;

            // Register the global table ONCE
            KTable<String, String> globalTable = builder.globalTable(
                dedupTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, String>>as(dedupTopic)
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.String())
            );

            // Input stream
            KStream<String, String> stream = builder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), Serdes.String())
            );

            // Deduplication logic using leftJoin
            KStream<String, String> deduped = stream.leftJoin(
                globalTable,
                (key, value) -> key,
                (value, existingHash) -> {
                    String newHash = computeHash(value);
                    return (existingHash == null || !existingHash.equals(newHash)) ? value : null;
                }
            ).filter((k, v) -> v != null);

            // Write unique records to output topic
            deduped.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            // Write hash values to dedup topic
            deduped.mapValues(DedupStreamsApplication::computeHash)
                   .to(dedupTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        return null;
    }

    private static String computeHash(String json) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(hash);
        } catch (Exception e) {
            return "";
        }
    }
}
