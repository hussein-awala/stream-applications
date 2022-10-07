package spark.stream.runners;

import com.sampullara.cli.Args;
import com.sampullara.cli.Argument;
import conf.HudiConf;
import conf.SparkConfBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import spark.stream.dataset_loader.StreamDatasetLoader;

public class KafkaToHudi {

  @Argument(alias = "t", description = "The topic name", required = true)
  private String topicName;

  @Argument(alias = "h", description = "The Hudi table name", required = true)
  private String hudiTableName;

  @Argument(description = "The hive database name to sync with")
  private String hiveDatabaseName;

  @Argument(alias = "k", description = "The Hudi table name keys", required = true)
  private String hudiKeys;

  @Argument(alias = "c", description = "The Hudi table name precombine key", required = true)
  private String hudiPrecombineKey;

  @Argument(alias = "p", description = "The Hudi table name partitions keys", required = true)
  private String partitionsKeys;

  @Argument(description = "The kafka topic json value schema path")
  private String schemaPath;

  @Argument(description = "The Hudi table name partitions keys")
  private Integer trigger;

  @Argument(description = "A flag used to deserialize avro values")
  private Boolean deserializeAvro;

  public static void main(String[] args)
      throws StreamingQueryException, RestClientException, IOException {
    KafkaToHudi kafkaToHudi = new KafkaToHudi();
    Args.usage(kafkaToHudi);
    List<String> extra = Args.parse(kafkaToHudi, args);

    SparkConf sparkConf =
        new SparkConfBuilder("Kafka Stream to Hudi", "local[1]")
            .addS3Conf()
            .addHudiConf()
            .addHiveConf()
            .build();

    SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();

    boolean deserializeAvro = kafkaToHudi.deserializeAvro != null && kafkaToHudi.deserializeAvro;

    Dataset<Row> df =
        new StreamDatasetLoader()
            .getDataset(spark, kafkaToHudi.topicName, deserializeAvro, kafkaToHudi.schemaPath);

    String hudiTableName = kafkaToHudi.hudiTableName;
    String hudiTablePath = String.format("s3a://spark/data/hudi/%s", hudiTableName);

    Map<String, String> hudiTableOptions =
        HudiConf.createHudiConf(
            hudiTableName,
            kafkaToHudi.hudiKeys,
            kafkaToHudi.hudiPrecombineKey,
            kafkaToHudi.partitionsKeys);
    if (kafkaToHudi.hiveDatabaseName != null)
      HudiConf.addHiveSyncConf(
          hudiTableOptions, kafkaToHudi.hiveDatabaseName, kafkaToHudi.partitionsKeys);

    int trigger = (kafkaToHudi.trigger == null) ? 120 : kafkaToHudi.trigger;

    df.writeStream()
        .format("hudi")
        .outputMode("append")
        .trigger(Trigger.ProcessingTime(String.format("%d seconds", trigger)))
        .option("checkpointLocation", String.format("s3a://spark/checkpoints/%s", hudiTableName))
        .options(hudiTableOptions)
        .start(hudiTablePath)
        .awaitTermination();
  }
}
