package spark.stream.dataset_loader;

import com.amazonaws.services.s3.AmazonS3;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import utils.*;
import utils.fields.*;

@ExtendWith(SparkTestExtension.class)
@ExtendWith(KafkaTestExtension.class)
@ExtendWith(MinioTestExtension.class)
@Testcontainers
public class StreamDatasetLoaderTest {
  @SparkSessionField public SparkSession sparkSession;

  @KafkaContainerField public KafkaContainer kafkaContainer;

  @KafkaAdminClientField public AdminClient kafkaAdminClient;

  @KafkaAvroConsumerField public KafkaConsumer<String, GenericRecord> kafkaConsumer;

  @KafkaAvroProducerField public KafkaProducer<String, GenericRecord> kafkaProducer;

  @MinioClientField public AmazonS3 minioClient;

  @Test
  void Given_SparkExtension_When_CallSparkSession_Then_ShouldNotBeNull() {
    Assertions.assertNotNull(sparkSession);
  }

  @Test
  void GivenTopicNameAndDeserializeAvroTrue_thenLoadDatasetAndWriteToS3()
      throws ExecutionException, InterruptedException, RestClientException, IOException,
          StreamingQueryException {

    String topicName = "test-avro-topic";

    Schema schema =
        SchemaBuilder.record("TestRecord")
            .namespace("spark.stream.dataset_loader")
            .fields()
            .requiredInt("x")
            .requiredBoolean("y")
            .requiredString("z")
            .endRecord();
    GenericRecord testRecord = new GenericData.Record(schema);
    testRecord.put("x", 1);
    testRecord.put("y", true);
    testRecord.put("z", "test string");
    ProducerRecord<String, GenericRecord> testMsg =
        new ProducerRecord<>(topicName, "test_key", testRecord);
    kafkaProducer.send(testMsg);

    Dataset<Row> streamDf =
        new StreamDatasetLoader("http://localhost:28081", kafkaContainer.getBootstrapServers())
            .getDataset(sparkSession, topicName, true, null);

    List<String> columns = Arrays.asList(streamDf.columns());

    Assertions.assertTrue(columns.containsAll(Arrays.asList("x", "y", "z")));

    String parquetPath = "s3a://test-bucket/test-parquet";
    String checkpointLocation = "s3a://test-bucket/checkpoint";

    streamDf
        .writeStream()
        .format("parquet")
        .outputMode(OutputMode.Append())
        .trigger(Trigger.Once())
        .option("checkpointLocation", checkpointLocation)
        .start(parquetPath)
        .awaitTermination();

    Dataset<Row> df = sparkSession.read().parquet(parquetPath);
    Row[] rows = (Row[]) df.collect();
    Assertions.assertEquals(1, rows.length);
    Row record = rows[0];
    Assertions.assertEquals(1, record.getInt(columns.indexOf("x")));
    Assertions.assertEquals(true, record.getBoolean(columns.indexOf("y")));
    Assertions.assertEquals("test string", record.getString(columns.indexOf("z")));
  }
}
