package com.example;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.Properties;

public class GenericDeduplicationApp {
    public static void main(String[] args) throws IOException {
        String mappingFile = "dedup-mapping.properties";
        DedupTopicMapper mapper = new DedupTopicMapper(mappingFile);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "dedup-streams-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        for (String inputTopic : mapper.getAllInputTopics()) {
            DedupTopicMapper.TopicMapping mapping = mapper.getMapping(inputTopic);
            String dedupTopic = mapping.dedupTopic;
            String outputTopic = mapping.outputTopic;

            KStream<String, String> stream = builder.stream(inputTopic);
            stream.filter((key, value) -> shouldEmit(key, value))
                  .to(outputTopic);

            stream.mapValues(GenericDeduplicationApp::computeHash)
                  .to(dedupTopic);
        }

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
    }

    private static boolean shouldEmit(String key, String value) {
        // Always emit for simplicity; add dedup logic here
        return true;
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
