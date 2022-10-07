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

public class JoinTable2WithHudiTable {

  public static void main(String[] args)
      throws RestClientException, IOException, StreamingQueryException {
    SparkConf sparkConf =
        new SparkConfBuilder("Upsert Kafka Stream to Hudi", "local[1]")
            .addS3Conf()
            .addHudiConf()
            .addHiveConf()
            .build();

    SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

    String hudiTableName1 = "table1";
    String hudiDbName1 = "hudi_stream";
    String hudiTablePath1 = String.format("s3a://spark/data/hudi/%s", hudiTableName1);
    String tableKey1 = "user_id";
    String precombineKey1 = "kafka_timestamp";
    String partitionKeys1 = "";
    String writeOperation1 = "upsert";

    String hudiTableNameRes = "res_table";
    String hudiDbNameRes = "hudi_stream";
    String hudiTablePathRes = String.format("s3a://spark/data/hudi/%s", hudiTableNameRes);
    String tableKeyRes = "table2_user_id";
    String precombineKeyRes = "table2_kafka_timestamp";
    String partitionKeysRes = "";
    String writeOperationRes = "upsert";

    Map<String, String> hudiTableOptions1 =
        HudiConf.createHudiConf(
            hudiTableName1, tableKey1, precombineKey1, partitionKeys1, writeOperation1);
    HudiConf.addHiveSyncConf(hudiTableOptions1, hudiDbName1, partitionKeys1);

    Map<String, String> hudiTableOptionsRes =
        HudiConf.createHudiConf(
            hudiTableNameRes, tableKeyRes, precombineKeyRes, partitionKeysRes, writeOperationRes);
    HudiConf.addHiveSyncConf(hudiTableOptionsRes, hudiDbNameRes, partitionKeysRes);

    Dataset<Row> table1Df = new StreamDatasetLoader().getDataset(spark, "table1");
    Dataset<Row> table2Df = new StreamDatasetLoader().getDataset(spark, "table2");

    table1Df
        .writeStream()
        .format("hudi")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", hudiTableName1))
        .options(hudiTableOptions1)
        .start(hudiTablePath1);

    Dataset<Row> table1HudiTable =
        spark.read().table(String.format("%s.%s", hudiDbName1, hudiTableName1));

    Dataset<Row> joinedDf =
        table2Df
            .as("table2")
            .join(
                table1HudiTable.as("table1"),
                functions.expr("table2.user_id = table1.user_id"),
                "left_outer")
            .select(
                table1HudiTable.col("user_id").as("table1_user_id"),
                table1HudiTable.col("kafka_timestamp").as("table1_kafka_timestamp"),
                table1HudiTable.col("feature1"),
                table2Df.col("user_id").as("table2_user_id"),
                table2Df.col("kafka_timestamp").as("table2_kafka_timestamp"),
                table2Df.col("feature2"));

    joinedDf
        .writeStream()
        .format("hudi")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime("30 seconds"))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", hudiTableNameRes))
        .options(hudiTableOptionsRes)
        .start(hudiTablePathRes);
    spark.streams().awaitAnyTermination();
  }
}
