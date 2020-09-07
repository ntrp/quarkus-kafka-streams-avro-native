package de.porsche.mymessaging.custcomm.enricher.avro;

import io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SpecificKavroDeserializer<R extends SpecificRecord>
        extends AbstractKafkaAvroDeserializer implements Deserializer<R> {

    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private final Class<R> schemaClass;
    private final Schema schema;

    public SpecificKavroDeserializer(Schema schema, Class<R> schemaClass) {
        this.schemaClass = schemaClass;
        this.schema = schema;
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configure(new KafkaAvroDeserializerConfig(configs));
    }

    @Override
    public R deserialize(String topic, byte[] data) {
        return (R) deserialize(topic, false, data, schema);
    }

    protected Object deserialize(String topic, Boolean isKey, byte[] payload, Schema readerSchema)
            throws SerializationException {
        if (payload == null) {
            return null;
        } else {
            final ByteBuffer buffer = getByteBuffer(payload);
            final int schemaId = buffer.getInt();
            DatumReader<?> reader = new SpecificDatumReader<>(schemaClass);
            int length = buffer.limit() - 1 - 4;
            int start = buffer.position() + buffer.arrayOffset();
            try {
                return reader
                        .read(null, this.decoderFactory.binaryDecoder(buffer.array(), start, length, null));
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