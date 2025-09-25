package com.example.kafka.connect.transforms;

import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.transforms.Transformation;
import java.util.Map;

public class DeleteOnNullValue<R extends ConnectRecord<R>> implements Transformation<R> {
    @Override
    public R apply(R record) {
        if (record.value() == null) {
            Headers headers = record.headers();
            headers.add("delete", "true", null);
        }
        return record;
    }

    @Override
    public void configure(Map<String, ?> configs) {}
    @Override
    public void close() {}
    @Override
    public org.apache.kafka.common.config.ConfigDef config() {
        return new org.apache.kafka.common.config.ConfigDef();
    }
}