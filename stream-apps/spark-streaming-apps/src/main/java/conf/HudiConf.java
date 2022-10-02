package conf;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.keygen.ComplexKeyGenerator;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;

public class HudiConf {

  public static Map<String, String> createHudiConf(
      String hudiTableName, String keyFields, String precombineField, String partitionFields) {
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
    String hudiKeyGeneratorClass;
    if (Objects.equals(keyFields, "")) {
      hudiKeyGeneratorClass = NonpartitionedKeyGenerator.class.getName();
    } else {
      hudiKeyGeneratorClass = ComplexKeyGenerator.class.getName();
    }
    hudiTableOptions.put("hoodie.datasource.write.keygenerator.class", hudiKeyGeneratorClass);
    return hudiTableOptions;
  }

  public static void addHiveSyncConf(
      Map<String, String> hudiTableOptions, String hudiTableDb, String partitionKeys) {
    String hivePartitionExtractorClass;
    if (Objects.equals(partitionKeys, "")) {
      hivePartitionExtractorClass = NonPartitionedExtractor.class.getName();
    } else {
      hivePartitionExtractorClass = MultiPartKeysValueExtractor.class.getName();
    }
    hudiTableOptions.put("hoodie.datasource.hive_sync.enable", "true");
    hudiTableOptions.put("hoodie.datasource.hive_sync.mode", "hms");
    hudiTableOptions.put("hoodie.datasource.hive_sync.partition_fields", partitionKeys);
    hudiTableOptions.put("hoodie.datasource.hive_sync.use_jdbc", "false");
    hudiTableOptions.put("hoodie.datasource.hive_sync.metastore.uris", "thrift://localhost:9083");
    hudiTableOptions.put("hoodie.datasource.hive_sync.auto_create_database", "true");
    hudiTableOptions.put("hoodie.datasource.hive_sync.database", hudiTableDb);
    hudiTableOptions.put(
        "hoodie.datasource.hive_sync.partition_extractor_class", hivePartitionExtractorClass);
  }

  public static Map<String, String> createHudiConf(
      String hudiTableName,
      String keyFields,
      String precombineField,
      String partitionFields,
      String operation) {
    String writeOperation;
    switch (operation) {
      case "insert":
        writeOperation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL();
        break;
      case "bulk_insert":
        writeOperation = DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL();
        break;
      case "insert_overwrite":
        writeOperation = DataSourceWriteOptions.INSERT_OVERWRITE_OPERATION_OPT_VAL();
        break;
      case "upsert":
        writeOperation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL();
        break;
      case "delete":
        writeOperation = DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL();
        break;
      case "delete_partition":
        writeOperation = DataSourceWriteOptions.DELETE_PARTITION_OPERATION_OPT_VAL();
        break;
      default:
        throw new RuntimeException(String.format("The operation %s is not supported", operation));
    }
    Map<String, String> conf =
        HudiConf.createHudiConf(hudiTableName, keyFields, precombineField, partitionFields);
    conf.put(DataSourceWriteOptions.OPERATION().key(), writeOperation);
    return conf;
  }
}
