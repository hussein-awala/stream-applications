package spark.stream.dataset_loader;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import utils.*;

@ExtendWith(SparkTestExtension.class)
@ExtendWith(KafkaTestExtension.class)
@Testcontainers
public class StreamDatasetLoader {
  @SparkSessionField public SparkSession sparkSession;

  @KafkaContainerField public KafkaContainer kafkaContainer;

  @KafkaConsumerField public KafkaConsumer kafkaConsumer;

  @KafkaProducerField public KafkaProducer kafkaProducer;

  @Test
  void Given_SparkExtension_When_CallSparkSession_Then_ShouldNotBeNull() {
    Assertions.assertNotNull(sparkSession);
  }
}
