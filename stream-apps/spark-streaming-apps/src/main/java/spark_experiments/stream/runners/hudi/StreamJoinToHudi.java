package spark_experiments.stream.runners.hudi;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.expr;

import conf.HudiConf;
import conf.SparkConfBuilder;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
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

public class StreamJoinToHudi {
  public static void main(String[] args)
      throws StreamingQueryException, RestClientException, IOException {
    SparkConf sparkConf =
        new SparkConfBuilder("Kafka Stream to Hudi", "local[1]")
            .addS3Conf()
            .addHudiConf()
            .addHiveConf()
            .build();
    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

    String table1Topic = "table1";
    String subject1ValueName = table1Topic + "-value";
    String table2Topic = "table2";
    String subject2ValueName = table2Topic + "-value";

    String schemaRegistryAddr = "http://localhost:8081";
    RestService schemaRegistry = new RestService(schemaRegistryAddr);

    String valueSchema1Str = schemaRegistry.getLatestVersion(subject1ValueName).getSchema();
    String valueSchema2Str = schemaRegistry.getLatestVersion(subject2ValueName).getSchema();

    Dataset<Row> df1 =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", table1Topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(
                from_avro(expr("substring(value, 6, length(value)-5)"), valueSchema1Str)
                    .alias("value"))
            .select("value.*");

    Dataset<Row> df2 =
        spark
            .readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", table2Topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(
                from_avro(expr("substring(value, 6, length(value)-5)"), valueSchema2Str)
                    .alias("value"))
            .select("value.*");

    Dataset<Row> joinedDf =
        df1.join(df2, df1.col("user_id").equalTo(df2.col("user_id")), "full_outer")
            .select(
                df1.col("event_timestamp").alias("event_timestamp1"),
                df2.col("event_timestamp").alias("event_timestamp2"),
                functions
                    .when(df1.col("user_id").isNotNull(), df1.col("user_id"))
                    .otherwise(df2.col("user_id"))
                    .alias("user_id"),
                df1.col("feature1"),
                df2.col("feature2"));
    String hudiTableName = "joined-table-stream";
    String hudiTablePath = String.format("s3a://spark/data/hudi/%s", hudiTableName);

    Map<String, String> hudiTableOptions =
        HudiConf.createHudiConf(hudiTableName, "user_id", "event_timestamp1,event_timestamp2", "");

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
