package conf;

import org.apache.hudi.DataSourceWriteOptions;

import java.util.HashMap;
import java.util.Map;

public class HudiConf {

    public static Map<String, String> createHudiConf(String hudiTableName, String keyFields, String precombineField, String partitionFields){
        Map<String, String> hudiTableOptions = new HashMap<>();
        hudiTableOptions.put("hoodie.insert.shuffle.parallelism", "2");
        hudiTableOptions.put("hoodie.upsert.shuffle.parallelism", "2");
        hudiTableOptions.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), keyFields);
        hudiTableOptions.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), precombineField);
        hudiTableOptions.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionFields);
        hudiTableOptions.put(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING().key(), "true");
        hudiTableOptions.put(DataSourceWriteOptions.TABLE_NAME().key(), hudiTableName);
        hudiTableOptions.put("hoodie.table.name", hudiTableName);
        hudiTableOptions.put("hoodie.parquet.compression.codec", "zstd");
        hudiTableOptions.put("hoodie.datasource.hive_sync.enable", "true");
        hudiTableOptions.put("hoodie.datasource.hive_sync.mode", "hms");
        hudiTableOptions.put("hoodie.datasource.hive_sync.metastore.uris", "thrift://localhost:9083");
        hudiTableOptions.put("hoodie.datasource.hive_sync.auto_create_database", "true");
        return hudiTableOptions;
    }
}
