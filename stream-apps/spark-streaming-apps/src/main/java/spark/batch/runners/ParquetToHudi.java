package spark.batch.runners;

import conf.HudiConf;
import conf.SparkConfBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Map;

public class ParquetToHudi {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConfBuilder("Parquet to Hudi table", "local[2]")
                .addHiveConf()
                .addS3Conf()
                .addHudiConf()
                .build();
        SparkSession spark = SparkSession
                .builder()
                .config(sparkConf)
                .enableHiveSupport()
                .getOrCreate();

        String parquetPath = "s3a://spark/data/power-consumption-parquet";
        String hudiTableName = "power_consumption";
        String hudiTablePath = String.format("s3a://spark/data/hudi/%s", hudiTableName);
        String hudiDbName = "hudi_batch";
        String tableKey = "Global_active_power";
        String precombineKey = "Global_reactive_power";
        String partitionKeys = "Date";

        Dataset<Row> df = spark.read().parquet(parquetPath);

        Map<String, String> hudiTableOptions = HudiConf.createHudiConf(
                hudiTableName,
                tableKey,
                precombineKey,
                partitionKeys
        );
        HudiConf.addHiveSyncConf(hudiTableOptions, hudiDbName, partitionKeys);

        df.limit(20).write().format("hudi")
                .options(hudiTableOptions)
                .mode(SaveMode.Overwrite)
                .save(hudiTablePath);
    }
}
