package spark.stream.dataset_loader;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.*;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;

public class StreamDatasetLoader {
  private String schemaRegistryAddr;

  private String bootstrapServer;

  public StreamDatasetLoader() {
    this.schemaRegistryAddr = "http://localhost:8081";
    this.bootstrapServer = "localhost:9092";
  }

  public StreamDatasetLoader(String schemaRegistryAddr, String bootstrapServer) {
    this.schemaRegistryAddr = schemaRegistryAddr;
    this.bootstrapServer = bootstrapServer;
  }

  /**
   * A method used for backward compatibility: deserialize avro by default
   *
   * @param sparkSession: the spark session
   * @param topicName: the kafka topic name to read
   * @return a transformed dataframe for the kafka topic
   * @throws RestClientException
   * @throws IOException
   */
  public Dataset<Row> getDataset(SparkSession sparkSession, String topicName)
      throws RestClientException, IOException {
    return getDataset(sparkSession, topicName, true, null);
  }

  public Dataset<Row> getDataset(
      SparkSession sparkSession, String topicName, boolean deserializeAvro, String jsonSchema)
      throws RestClientException, IOException {

    Dataset<Row> df =
        sparkSession
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", bootstrapServer)
            .option("subscribe", topicName)
            .option("startingOffsets", "earliest")
            .load();

    if (deserializeAvro) {
      String subjectValueName = topicName + "-value";

      RestService schemaRegistry = new RestService(this.schemaRegistryAddr);
      String valueSchemaStr = schemaRegistry.getLatestVersion(subjectValueName).getSchema();

      df =
          df.withColumn(
              "value", from_avro(expr("substring(value, 6, length(value)-5)"), valueSchemaStr));
    } else {
      File schemeFile = new File(jsonSchema);
      DataType schema =
          StructType.fromJson(FileUtils.readFileToString(schemeFile, StandardCharsets.UTF_8));
      df = df.withColumn("value", from_json(col("value").cast("STRING"), schema));
    }

    return df.select(
        col("timestamp").alias("kafka_timestamp"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("key").alias("kafka_key"),
        col("value.*"));
  }
}
