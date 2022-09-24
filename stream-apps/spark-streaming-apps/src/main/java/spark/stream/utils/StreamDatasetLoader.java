package spark.stream.utils;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;

public class StreamDatasetLoader {
    static String schemaRegistryAddr = "http://localhost:8081";

    public static Dataset<Row> getDataset(SparkSession sparkSession, String topicName) throws RestClientException, IOException {

        String subjectValueName = topicName + "-value";

        RestService schemaRegistry = new RestService(StreamDatasetLoader.schemaRegistryAddr);
        String valueSchemaStr = schemaRegistry.getLatestVersion(subjectValueName).getSchema();

        return sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topicName)
                .option("startingOffsets", "earliest")
                .load()
                .withColumn(
                        "value",
                        from_avro(expr("substring(value, 6, length(value)-5)"), valueSchemaStr)
                )
                .select(
                        col("timestamp").alias("kafka_timestamp"),
                        col("partition").alias("kafka_partition"),
                        col("offset").alias("kafka_offset"),
                        col("key").alias("kafka_key"),
                        col("value.*")
                );
    }

}
