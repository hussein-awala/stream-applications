package spark.stream.utils;

import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.spark.sql.*;

import java.io.IOException;

import static org.apache.spark.sql.avro.functions.from_avro;
import static org.apache.spark.sql.functions.*;

public class PowerConsumptionLoader {
    static String schemaRegistryAddr = "http://localhost:8081";


    public static Dataset<Row> getDataset(SparkSession sparkSession) throws RestClientException, IOException {

        String topicName = "power-consumption";
        String subjectValueName = topicName + "-value";

        RestService schemaRegistry = new RestService(PowerConsumptionLoader.schemaRegistryAddr);
        String valueSchemaStr = schemaRegistry.getLatestVersion(subjectValueName).getSchema();

        return sparkSession
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", topicName)
                .option("startingOffsets", "earliest")
                .load()
                .withColumn(
                        "value",
                        from_avro(expr("substring(value, 6, length(value)-5)"), valueSchemaStr)
                )
                .select(
                        col("timestamp").alias("kafka_timestamp"), col("value.*")
                )
                .withColumn(
                        "Date",
                        date_format(
                                make_date(
                                        element_at(split(col("Date"), "/"), 3).cast("int"),
                                        element_at(split(col("Date"), "/"), 2).cast("int"),
                                        element_at(split(col("Date"), "/"), 1).cast("int")
                                ),
                                "yyyy-MM-dd"
                        )
                );
    }

}
