@SpringBootApplication
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

    // ✅ Replace field injection with this
    private KafkaStreams kafkaStreams;

    // ✅ Listen for KafkaStreams bean when it's ready
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

            builder.globalTable(dedupTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(dedupTopic).withLoggingDisabled());

            KStream<String, String> stream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

            stream.filter((key, value) -> shouldEmit(key, value, dedupTopic))
                  .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

            stream.mapValues(DedupStreamsApplication::computeHash)
                  .to(dedupTopic, Produced.with(Serdes.String(), Serdes.String()));
        }

        return null;
    }

    private boolean shouldEmit(String key, String value, String storeName) {
        if (kafkaStreams == null) return true; // fallback if not ready

        ReadOnlyKeyValueStore<String, String> store = storeCache.computeIfAbsent(storeName, name ->
            kafkaStreams.store(name, QueryableStoreTypes.keyValueStore())
        );

        String newHash = computeHash(value);
        String oldHash = store.get(key);

        return oldHash == null || !newHash.equals(oldHash);
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
