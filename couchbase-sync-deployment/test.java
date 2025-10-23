package com.hwow.streams.apps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Component;

import com.hwow.streams.configuration.Constants;
import com.hwow.streams.model.Medication;

@Component
public class MedicationsDeduper {
    private static final Logger logger = LoggerFactory.getLogger(MedicationsDeduper.class);
    
    @Autowired
    private StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    
    private ReadOnlyKeyValueStore<String, Medication> medicationsStore;
    
    @Bean
    public KStream<String, Medication> dedupMedications(StreamsBuilder kStreamBuilder) {
	kStreamBuilder.globalTable(Constants.MEDICATIONS_OUTPUT_TOPIC, Consumed.with(Serdes.String(), Constants.MEDICATIONS_SERDE), Materialized
		.<String, Medication, KeyValueStore<Bytes, byte[]>>as(Constants.MEDICATIONS_STORE).withLoggingDisabled());
	
	KStream<String, Medication> medicationsStream = kStreamBuilder.stream(Constants.MEDICATIONS_INPUT_TOPIC, Consumed.with(Serdes.String(), Constants.MEDICATIONS_SERDE));
	medicationsStream.filter((k,v) -> removeDuplicates(k,v)).to(Constants.MEDICATIONS_OUTPUT_TOPIC, Produced.with(Serdes.String(), Constants.MEDICATIONS_SERDE));
	return medicationsStream;
    }
    
    private boolean removeDuplicates(String key, Medication newMed) {
	if(null == medicationsStore) {
	    KafkaStreams streams = streamsBuilderFactoryBean.getKafkaStreams();
	    medicationsStore = streams.store(Constants.MEDICATIONS_STORE, QueryableStoreTypes.<String, Medication>keyValueStore());
	}
	Medication existingMed = medicationsStore.get(key);
	if(null != newMed && !newMed.equals(existingMed)) {
	    logger.info("Found non-duplicate. Sending to Kafka topic. Medication key {}", newMed.getId());
	    return true;
	}
	return false;
    }
}