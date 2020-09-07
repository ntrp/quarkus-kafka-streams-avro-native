package de.porsche.mymessaging.custcomm.enricher.avro;

import de.porsche.mymessaging.custcomm.kafka.dto.Message;
import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;


public class MessageDeserializer
        extends AbstractKafkaAvroDeserializer implements Deserializer<Message> {

    private final DecoderFactory decoderFactory = DecoderFactory.get();

    public MessageDeserializer() {
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public Message deserialize(String topic, byte[] data)
            throws SerializationException {
        if (data == null) {
            return null;
        } else {
            final ByteBuffer buffer = getByteBuffer(data);
            final int schemaId = buffer.getInt();
            try {
                return Message.getDecoder().decode(buffer);
            } catch (RuntimeException | IOException e) {
                throw new SerializationException("Error deserializing Avro message for id " + schemaId, e);
            }
        }
    }

    public void close() {
    }

    protected ByteBuffer getByteBuffer(byte[] payload) {
        ByteBuffer buffer = ByteBuffer.wrap(payload);
        if (buffer.get() != 0) {
            throw new SerializationException("Unknown magic byte!");
        } else {
            return buffer;
        }
    }

}

