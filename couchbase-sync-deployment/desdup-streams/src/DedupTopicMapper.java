package com.path.stream.app;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

/**
 * DedupTopicMapper is responsible for reading a topic mapping configuration file
 * (e.g., dedup-mapping.properties) from the classpath.
 * These mappings are used by the Kafka Streams topology to dynamically
 * wire deduplication pipelines between input and output topics.
 */
public class DedupTopicMapper {

    // Stores all topic mappings from the properties file
    private final Properties mappings = new Properties();

    /**
     * Loads the mapping file from the classpath.
     *
     * @param filePath The name of the mapping file, e.g., "dedup-mapping.properties"
     */
    public DedupTopicMapper(String filePath) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(filePath)) {

            // Ensure the file actually exists in classpath
            if (input == null) {
                throw new IllegalArgumentException(
                        "Mapping file not found in classpath: " + filePath
                );
            }

            // Load all mappings from the properties file
            mappings.load(input);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load topic mapping file: " + filePath, e);
        }
    }

    /**
     * Returns all available input topic names defined in the mapping file.
     * Each property key is treated as an input topic name.
     */
    public Set<String> getAllInputTopics() {
        return mappings.stringPropertyNames();
    }

    /**
     * Retrieves the output topic corresponding to the given input topic.
     *
     * @param inputTopic Input topic name
     * @return TopicMapping object containing output topic
     */
    public TopicMapping getMapping(String inputTopic) {
        String outputTopic = mappings.getProperty(inputTopic);
        if (outputTopic == null) {
            throw new IllegalArgumentException("No mapping found for input topic: " + inputTopic);
        }
        return new TopicMapping(outputTopic);
    }

    /**
     * Represents a simple mapping of input → output topic.
     */
    public static class TopicMapping {
        public final String outputTopic;

        public TopicMapping(String outputTopic) {
            this.outputTopic = outputTopic;
        }
    }
}
