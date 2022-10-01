package spark_experiments.utils;

import conf.SparkConfBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class HiveDatabasesCreator {
  public static void main(String[] args) {
    SparkConf sparkConf =
        new SparkConfBuilder("CREATE Hive Databases", "local[1]")
            .addHiveConf()
            .addS3Conf()
            .addHudiConf()
            .build();
    SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    spark.sql("CREATE DATABASE IF NOT EXISTS hudi_stream").collect();
    spark.sql("CREATE DATABASE IF NOT EXISTS hudi_batch").collect();
    spark.sql("SHOW DATABASES").show(10);
    spark.sql("SHOW TABLES FROM hudi_stream").show(10);
    spark.sql("SHOW TABLES FROM hudi_batch").show(10);
  }
}
