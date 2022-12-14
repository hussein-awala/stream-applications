package spark_experiments.stream.runners;

import com.stream.apps.lib.conf.SparkConfBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import spark_experiments.stream.dataset_loader.PowerConsumptionLoader;

public class KafkaToConsole {
  public static void main(String[] args)
      throws TimeoutException, StreamingQueryException, RestClientException, IOException {
    SparkConf sparkConf = new SparkConfBuilder("Kafka Stream to Console", "local[1]").build();

    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

    Dataset<Row> df = PowerConsumptionLoader.getDataset(spark);

    df.writeStream()
        .format("console")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .start()
        .awaitTermination();
  }
}
