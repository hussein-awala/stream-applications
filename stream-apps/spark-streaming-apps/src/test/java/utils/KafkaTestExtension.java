package utils;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import utils.fields.KafkaAdminClientField;
import utils.fields.KafkaConsumerField;
import utils.fields.KafkaContainerField;
import utils.fields.KafkaProducerField;

public class KafkaTestExtension extends AbstractTestExtension
    implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback {

  private KafkaContainer kafkaContainer;

  private AdminClient kafkaAdminClient;

  private KafkaConsumer kafkaConsumer;

  private KafkaProducer kafkaProducer;

  @Override
  public void beforeAll(ExtensionContext context) {
    kafkaContainer =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
            .withEmbeddedZookeeper()
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(30)));

    kafkaContainer.start();

    Properties adminProperties = new Properties();
    adminProperties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    kafkaAdminClient = AdminClient.create(adminProperties);

    Properties consumerProperties = new Properties();
    consumerProperties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    kafkaConsumer = new KafkaConsumer(consumerProperties);

    Properties producerProperties = new Properties();
    producerProperties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProducer = new KafkaProducer(producerProperties);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    kafkaConsumer.close();
    kafkaProducer.close();
    kafkaContainer.stop();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws IllegalAccessException {
    patchField(context, KafkaContainerField.class, kafkaContainer);
    patchField(context, KafkaAdminClientField.class, kafkaAdminClient);
    patchField(context, KafkaConsumerField.class, kafkaConsumer);
    patchField(context, KafkaProducerField.class, kafkaProducer);
  }

  @Override
  public void afterEach(ExtensionContext context) throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
    kafkaAdminClient.deleteTopics(listTopicsResult.names().get()).all().get();
  }
}
