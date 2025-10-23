package com.path.stream.app;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class DedupTopicMapper {
    private final Properties mappings = new Properties();

    public DedupTopicMapper(String filePath) {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            mappings.load(fis);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mapping file", e);
        }
    }

    public Set<String> getAllInputTopics() {
        return mappings.stringPropertyNames();
    }

    public TopicMapping getMapping(String inputTopic) {
        String value = mappings.getProperty(inputTopic);
        String[] parts = value.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid mapping for topic: " + inputTopic);
        }
        return new TopicMapping(parts[0].trim(), parts[1].trim());
    }

    public static class TopicMapping {
        public final String dedupTopic;
        public final String outputTopic;

        public TopicMapping(String dedupTopic, String outputTopic) {
            this.dedupTopic = dedupTopic;
            this.outputTopic = outputTopic;
        }
    }
}
