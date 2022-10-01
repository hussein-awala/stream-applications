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
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import spark.stream.dataset_loader.StreamDatasetLoader;

public class JoinTables {

  public static void main(String[] args)
      throws RestClientException, IOException, StreamingQueryException {
    SparkConf sparkConf =
        new SparkConfBuilder("Upsert Kafka Stream to Hudi", "local[1]")
            .addS3Conf()
            .addHudiConf()
            .addHiveConf()
            .build();

    SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    String hudiTableName = "join_tables_stream";
    String hudiDbName = "hudi_stream";
    String hudiTablePath = String.format("s3a://spark/data/hudi/%s", hudiTableName);
    String tableKey = "table2_user_id";
    String precombineKey = "table2_kafka_timestamp";
    String partitionKeys = "";
    String writeOperation = "upsert";

    Map<String, String> hudiTableOptions =
        HudiConf.createHudiConf(
            hudiTableName, tableKey, precombineKey, partitionKeys, writeOperation);
    HudiConf.addHiveSyncConf(hudiTableOptions, hudiDbName, partitionKeys);

    Dataset<Row> table1Df = StreamDatasetLoader.getDataset(spark, "table1");
    Dataset<Row> table2Df = StreamDatasetLoader.getDataset(spark, "table2");
    Dataset<Row> joinedDf =
        table2Df
            .as("table2")
            .withWatermark("kafka_timestamp", "1 hour")
            .join(
                table1Df.as("table1").withWatermark("kafka_timestamp", "1 hour"),
                functions.expr(
                    "table2.user_id = table1.user_id"
                        + " AND table2.kafka_timestamp >= table1.kafka_timestamp"),
                "left_outer")
            .select(
                table1Df.col("user_id").as("table1_user_id"),
                table1Df.col("kafka_timestamp").as("table1_kafka_timestamp"),
                table1Df.col("feature1"),
                table2Df.col("user_id").as("table2_user_id"),
                table2Df.col("kafka_timestamp").as("table2_kafka_timestamp"),
                table2Df.col("feature2"));
    joinedDf
        .writeStream()
        .format("hudi")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", hudiTableName))
        .options(hudiTableOptions)
        .start(hudiTablePath)
        .awaitTermination();
  }
}
