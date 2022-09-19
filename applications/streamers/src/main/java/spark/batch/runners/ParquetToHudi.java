package spark.batch.runners;

import conf.SparkConfBuilder;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
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
                .getOrCreate();

        String parquetPath = "s3a://spark/data/power-consumption-parquet";

        String hudiTablePath = "s3a://spark/data/power-consumption-table";
        String hudiTableName = "power_consumption";

        Dataset<Row> df = spark.read().parquet(parquetPath);

        Map<String, String> hudiTableOptions = new HashMap<>();
        hudiTableOptions.put("hoodie.insert.shuffle.parallelism", "2");
        hudiTableOptions.put("hoodie.upsert.shuffle.parallelism", "2");
        hudiTableOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "Global_active_power");
        hudiTableOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "Global_reactive_power");
        hudiTableOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), "Date");
        hudiTableOptions.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
        hudiTableOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), hudiTableName);
        hudiTableOptions.put("hoodie.table.name", hudiTableName);
        hudiTableOptions.put("hoodie.parquet.compression.codec", "zstd");
        hudiTableOptions.put("hoodie.datasource.hive_sync.enable", "true");
        hudiTableOptions.put("hoodie.datasource.hive_sync.mode", "hms");
        hudiTableOptions.put("hoodie.datasource.hive_sync.metastore.uris", "thrift://localhost:9083");
        hudiTableOptions.put("hoodie.datasource.hive_sync.auto_create_database", "true");

        df.limit(20).write().format("hudi")
                .options(hudiTableOptions)
                .mode(SaveMode.Overwrite)
                .save(hudiTablePath);
    }
}
