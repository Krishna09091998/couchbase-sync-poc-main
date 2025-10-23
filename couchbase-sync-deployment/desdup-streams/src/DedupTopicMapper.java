package com.example;

import java.io.*;
import java.util.*;

public class DedupTopicMapper {
    private final Map<String, TopicMapping> topicMap = new HashMap<>();

    public DedupTopicMapper(String mappingFilePath) throws IOException {
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(mappingFilePath)) {
            props.load(input);
        }

        for (String inputTopic : props.stringPropertyNames()) {
            String[] parts = props.getProperty(inputTopic).split(",");
            if (parts.length == 2) {
                topicMap.put(inputTopic, new TopicMapping(parts[0], parts[1]));
            }
        }
    }

    public TopicMapping getMapping(String inputTopic) {
        return topicMap.get(inputTopic);
    }

    public Set<String> getAllInputTopics() {
        return topicMap.keySet();
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
