package spark.stream.utils;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;

import static org.apache.spark.sql.functions.*;

public class PowerConsumptionLoader {
    public static Dataset<Row> getDataset(SparkSession sparkSession) throws RestClientException, IOException {
        String topicName = "power-consumption";
        Dataset<Row> df = StreamDatasetLoader.getDataset(sparkSession, topicName);
        return df.drop("kafka_partition", "kafka_offset", "kafka_key")
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
