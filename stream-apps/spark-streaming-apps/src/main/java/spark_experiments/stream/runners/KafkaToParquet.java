package spark_experiments.stream.runners;

import conf.SparkConfBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import spark_experiments.stream.dataset_loader.PowerConsumptionLoader;

public class KafkaToParquet {
  public static void main(String[] args)
      throws StreamingQueryException, RestClientException, IOException {
    SparkConf sparkConf =
        new SparkConfBuilder("Kafka Stream to parquet", "local[1]").addS3Conf().build();

    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

    Dataset<Row> df = PowerConsumptionLoader.getDataset(spark);

    String parquet = "power-consumption-parquet";

    df.writeStream()
        .format("parquet")
        .partitionBy("Date")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", parquet))
        .start(String.format("s3a://spark/data/%s", parquet))
        .awaitTermination();
  }
}
