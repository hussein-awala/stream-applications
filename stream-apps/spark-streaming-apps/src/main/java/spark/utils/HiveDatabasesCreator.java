package spark.utils;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import conf.SparkConfBuilder;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class HiveDatabasesCreator {

  @Argument(alias = "d", description = "Hive database to create", required = true)
  private String hiveDatabaseName;

  public static void main(String[] args) {

    HiveDatabasesCreator hiveDatabasesCreator = new HiveDatabasesCreator();
    Args.usage(hiveDatabasesCreator);
    List<String> extra = Args.parse(hiveDatabasesCreator, args);

    SparkConf sparkConf =
        new SparkConfBuilder("CREATE Hive Databases", "local[1]")
            .addHiveConf()
            .addS3Conf()
            .addHudiConf()
            .build();
    SparkSession spark = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();
    spark
        .sql(
            String.format(
                "CREATE DATABASE IF NOT EXISTS %s", hiveDatabasesCreator.hiveDatabaseName))
        .collect();
    spark.sql("SHOW DATABASES").show(100);
  }
}
