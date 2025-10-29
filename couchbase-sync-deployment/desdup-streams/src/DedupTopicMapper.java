package com.path.stream.app;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.*;

public class DedupTopicMapper {
    private final Properties mappings = new Properties();

    public DedupTopicMapper(String filePath) {
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(filePath)) {
            mappings.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mapping file", e);
        }
    }

    public Set<String> getAllInputTopics() {
        return mappings.stringPropertyNames();
    }

    public TopicMapping getMapping(String inputTopic) {
        String value = mappings.getProperty(inputTopic);
        return new TopicMapping(value);
    }

    public static class TopicMapping {
        public final String outputTopic;
        public TopicMapping(String outputTopic) {
            this.outputTopic = outputTopic;
        }
    }
}
