package spark.stream.runners.kstrems;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Table1TopicTransformer {

    private static final Logger logger = LogManager.getLogger("KStream");
    private static String schemaRegistryUrl = "http://localhost:8081";

    public static void main(String[] args) {
        Table1TopicTransformer kafkaToKafka = new Table1TopicTransformer();

        Properties streamProperties = new Properties();
        streamProperties.put("application.id", "kafka-to-kafka");
        streamProperties.put("bootstrap.servers", "localhost:9092");

        KafkaStreams kafkaStreams = new KafkaStreams(
                kafkaToKafka.buildTopology(),
                streamProperties
        );
        kafkaStreams.start();
    }

    public Topology buildTopology() {

        String inputTopic = "table1";
        String outputTopic = "trans-table1";

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", Table1TopicTransformer.schemaRegistryUrl);

        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", Table1TopicTransformer.schemaRegistryUrl);
        final Serde<String> keyStringSerde = Serdes.String();
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false);

        StreamsBuilder builder = new StreamsBuilder();
        builder
                .stream(inputTopic, Consumed.with(keyStringSerde, valueGenericAvroSerde))
                .peek((k, v) -> logger.info("Observed event: {}", v))
                .mapValues(s -> {
                    double feature1 = (double) s.get("feature1");
                    s.put("feature1", feature1 * 1000);
                    return s;
                })
                .peek((k, v) -> logger.info("Transformed event: {}", v))
                .to(outputTopic, Produced.with(keyStringSerde, valueGenericAvroSerde));

        return builder.build();
    }
}
