package de.porsche.mymessaging.custcomm.enricher.avro;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

public class SpecificKavroSerializer<R extends SpecificRecord> extends AbstractKafkaAvroSerializer implements Serializer<R> {

    private final Schema schema;

    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public SpecificKavroSerializer(Schema schema) {
        this.schema = schema;
    }

    public void configure(Map<String, ?> configs, boolean isKey) {
        this.configure(new KafkaAvroSerializerConfig(configs));
    }

    public void close() {
    }

    @Override
    public byte[] serialize(String subject, R object) {
        if (object == null) {
            return null;
        } else {
            String restClientErrorMsg = "";

            try {
                final String subjectName = subject + "-value";

                int id;

                if (this.autoRegisterSchema) {
                    restClientErrorMsg = ": failed to register schema " + schema;
                    id = this.schemaRegistry.register(subjectName, schema);
                } else {
                    restClientErrorMsg = ": failed to retrieve schema " + schema;
                    id = this.schemaRegistry.getId(subjectName, schema);
                }

                ByteArrayOutputStream out = new ByteArrayOutputStream();
                out.write(0);
                out.write(ByteBuffer.allocate(4).putInt(id).array());
                BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, null);
                Object value = object instanceof NonRecordContainer
                        ? ((NonRecordContainer) object).getValue()
                        : object;
                SpecificDatumWriter writer = new SpecificDatumWriter(schema);
                writer.write(value, encoder);
                encoder.flush();

                byte[] bytes = out.toByteArray();
                out.close();
                return bytes;
            } catch (RuntimeException | IOException | RestClientException e) {
                throw new SerializationException("Error serializing Avro message" + restClientErrorMsg, e);
            }
        }
    }

}

