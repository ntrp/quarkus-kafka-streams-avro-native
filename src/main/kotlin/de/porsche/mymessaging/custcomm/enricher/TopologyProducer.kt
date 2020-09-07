package de.porsche.mymessaging.custcomm.enricher;

import de.porsche.mymessaging.custcomm.enricher.avro.MessageSerializer
import de.porsche.mymessaging.custcomm.enricher.avro.SpecificKavroDeserializer
import de.porsche.mymessaging.custcomm.kafka.dto.Message
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig.CLIENT_NAMESPACE
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.eclipse.microprofile.config.ConfigProvider
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.jboss.logging.Logger
import javax.enterprise.context.ApplicationScoped
import javax.enterprise.inject.Produces
import javax.inject.Inject

@ApplicationScoped
class TopologyProducer {

    private val LOG: Logger = Logger.getLogger(TopologyProducer::class.java)

    @ConfigProperty(name = "custcomm.topic.enrichment")
    lateinit var enrichmentTopicName: String

    @ConfigProperty(name = "custcomm.topic.accepted")
    lateinit var acceptedTopicName: String

    @Inject
    lateinit var enricher: Enricher

    @Produces
    fun buildTopology(): Topology {

        val builder = StreamsBuilder()

        val enriched: Produced<String, Message> = Produced.with(getKeySerDe(), getValueSerde())

        val stream: KStream<String, Message> = builder.stream(enrichmentTopicName, Consumed.with(getKeySerDe(), getValueSerde()))

        stream.peek { _: String, value: Message -> LOG.info("Message: " + value.getMessageId()) }

        val enrichedStream: KStream<String, Message> = stream.mapValues(enricher::enrich)

        enrichedStream.to(acceptedTopicName, enriched)

        return builder.build()
    }

    fun getKeySerDe(): Serdes.StringSerde = Serdes.StringSerde()

    fun getSchemaRegistrySSLConfig(): Map<String, String> {

        val config = ConfigProvider.getConfig()
        return config.propertyNames
                .filter { it.startsWith(CLIENT_NAMESPACE) }
                .map { it to config.getValue(it, String::class.java) }
                .toMap()
    }

    fun getValueSerde(): Serde<Message> {

        return Serdes.serdeFrom(MessageSerializer(), SpecificKavroDeserializer(Message.getClassSchema(), Message::class.java))
        //return SpecificAvroSerde<Message>()
        .apply {
            configure(
                    mutableMapOf(
                            "specific.avro.reader" to "true",
                            "auto.register.schemas" to "true",
                            "schema.reflection" to "false",
                    ).apply {
                        putAll(getSchemaRegistrySSLConfig())
                    },
                    false
            )
        }
    }
}
