package de.porsche.mymessaging.custcomm.enricher.avro;

import de.porsche.mymessaging.custcomm.kafka.dto.Message;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.HashMap;
import java.util.Map;

public class SpecificNativeAvroDeserializer implements Deserializer<Message> {
    private final KafkaAvroDeserializer inner;

    public SpecificNativeAvroDeserializer() {
        this.inner = new KafkaAvroDeserializer();
    }

    SpecificNativeAvroDeserializer(SchemaRegistryClient client) {
        this.inner = new KafkaAvroDeserializer(client);
    }

    public void configure(Map<String, ?> deserializerConfig, boolean isDeserializerForRecordKeys) {
        this.inner.configure(withSpecificAvroEnabled(deserializerConfig), isDeserializerForRecordKeys);
    }

    public Message deserialize(String topic, byte[] bytes) {
        return (Message) this.inner.deserialize(topic, bytes, Message.SCHEMA$);
    }

    public void close() {
        this.inner.close();
    }


    public static Map<String, Object> withSpecificAvroEnabled(Map<String, ?> config) {
        Map<String, Object> specificAvroEnabledConfig = config == null ? new HashMap() : new HashMap(config);
        specificAvroEnabledConfig.put("specific.avro.reader", true);
        return specificAvroEnabledConfig;
    }
}
