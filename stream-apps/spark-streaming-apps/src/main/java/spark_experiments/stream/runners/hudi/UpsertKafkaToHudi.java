package spark_experiments.stream.runners.hudi;

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
import spark.stream.dataset_loader.StreamDatasetLoader;

public class UpsertKafkaToHudi {
  public static void main(String[] args)
      throws StreamingQueryException, RestClientException, IOException {
    SparkConf sparkConf =
        new SparkConfBuilder("Upsert Kafka Stream to Hudi", "local[1]")
            .addS3Conf()
            .addHudiConf()
            .addHiveConf()
            .build();

    SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    Dataset<Row> df = StreamDatasetLoader.getDataset(spark, "joined-tables");

    String hudiTableName = "joined_tables_stream";
    String hudiDbName = "hudi_stream";
    String hudiTablePath = String.format("s3a://spark/data/hudi/%s", hudiTableName);
    String tableKey = "user_id";
    String precombineKey = "kafka_timestamp";
    String partitionKeys = "";
    String writeOperation = "upsert";

    Map<String, String> hudiTableOptions =
        HudiConf.createHudiConf(
            hudiTableName, tableKey, precombineKey, partitionKeys, writeOperation);
    HudiConf.addHiveSyncConf(hudiTableOptions, hudiDbName, partitionKeys);

    df.writeStream()
        .format("hudi")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", hudiTableName))
        .options(hudiTableOptions)
        .start(hudiTablePath)
        .awaitTermination();
  }
}
