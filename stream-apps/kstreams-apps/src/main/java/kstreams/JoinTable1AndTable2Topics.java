package kstreams;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JoinTable1AndTable2Topics {

  private static final Logger logger =
      LogManager.getLogger(JoinTable1AndTable2Topics.class.getName());
  private static String schemaRegistryUrl = "http://localhost:8081";

  public static void main(String[] args) throws IOException {
    JoinTable1AndTable2Topics kafkaToKafka = new JoinTable1AndTable2Topics();

    Properties streamProperties = new Properties();
    streamProperties.put("application.id", "stream-join");
    streamProperties.put("bootstrap.servers", "localhost:9092");

    KafkaStreams kafkaStreams = new KafkaStreams(kafkaToKafka.buildTopology(), streamProperties);
    kafkaStreams.start();
  }

  public Topology buildTopology() throws IOException {

    String table1Topic = "table1";
    String table2Topic = "table2";
    String joinedTopic = "joined-tables";

    ClassLoader classLoader = getClass().getClassLoader();
    InputStream joinedTopicSchemaIs = classLoader.getResourceAsStream("join/joinedTopic.avsc");
    Schema joinedTopicSchema = new Schema.Parser().parse(joinedTopicSchemaIs);

    final Properties streamsConfiguration = new Properties();
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamsConfiguration.put(
        StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    streamsConfiguration.put("schema.registry.url", JoinTable1AndTable2Topics.schemaRegistryUrl);

    final Map<String, String> serdeConfig =
        Collections.singletonMap(
            "schema.registry.url", JoinTable1AndTable2Topics.schemaRegistryUrl);
    final Serde<String> keyStringSerde = Serdes.String();
    final Serde<Integer> userIdKeySerde = Serdes.Integer();
    final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
    valueGenericAvroSerde.configure(serdeConfig, false);

    StreamsBuilder builder = new StreamsBuilder();
    final KStream<String, GenericRecord> table1Stream =
        builder.stream(table1Topic, Consumed.with(keyStringSerde, valueGenericAvroSerde));
    final KStream<String, GenericRecord> table2Stream =
        builder.stream(table2Topic, Consumed.with(keyStringSerde, valueGenericAvroSerde));
    final KTable<Integer, GenericRecord> table1Ktable =
        table1Stream
            .groupBy(
                (k, v) -> (int) v.get("user_id"),
                Grouped.with(userIdKeySerde, valueGenericAvroSerde))
            .aggregate(
                () -> null,
                (aggKey, newValue, aggValue) -> newValue,
                Materialized.<Integer, GenericRecord, KeyValueStore<Bytes, byte[]>>as(
                        "aggregated-stream-store")
                    .withKeySerde(userIdKeySerde)
                    .withValueSerde(valueGenericAvroSerde));
    KStream<Integer, GenericRecord> joinedStream =
        table2Stream
            .selectKey((k, v) -> (Integer) v.get("user_id"))
            .leftJoin(
                table1Ktable,
                (table2Value, table1Value) -> {
                  GenericRecord joinedValue = new GenericData.Record(joinedTopicSchema);
                  joinedValue.put("event_timestamp", table2Value.get("event_timestamp"));
                  joinedValue.put("user_id", table2Value.get("user_id"));
                  if (table1Value != null) joinedValue.put("feature1", table1Value.get("feature1"));
                  joinedValue.put("feature2", table2Value.get("feature2"));
                  return joinedValue;
                },
                Joined.keySerde(userIdKeySerde));
    joinedStream.to(joinedTopic, Produced.with(userIdKeySerde, valueGenericAvroSerde));
    return builder.build();
  }
}
