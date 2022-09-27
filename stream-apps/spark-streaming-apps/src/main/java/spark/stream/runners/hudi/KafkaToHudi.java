package spark.stream.runners.hudi;

import conf.HudiConf;
import conf.SparkConfBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import spark.stream.utils.PowerConsumptionLoader;

public class KafkaToHudi {
  public static void main(String[] args)
      throws StreamingQueryException, RestClientException, IOException {
    SparkConf sparkConf =
        new SparkConfBuilder("Kafka Stream to Hudi", "local[1]")
            .addS3Conf()
            .addHudiConf()
            .addHiveConf()
            .build();

    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

    Dataset<Row> df = PowerConsumptionLoader.getDataset(spark);

    String hudiTableName = "power-consumption-parquet-stream";
    String hudiTablePath = String.format("s3a://spark/data/hudi/%s", hudiTableName);

    Map<String, String> hudiTableOptions =
        HudiConf.createHudiConf(
            hudiTableName, "Global_active_power", "Global_reactive_power", "Date");

    df.writeStream()
        .format("hudi")
        .partitionBy("Date")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", hudiTableName))
        .options(hudiTableOptions)
        .start(hudiTablePath)
        .awaitTermination();
  }
}
