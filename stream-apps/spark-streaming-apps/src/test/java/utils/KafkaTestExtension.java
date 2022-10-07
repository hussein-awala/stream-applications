package utils;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.generic.GenericRecord;
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
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import utils.fields.*;

public class KafkaTestExtension extends AbstractTestExtension
    implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback, AfterAllCallback {

  private KafkaContainer kafkaContainer;

  private GenericContainer schemaRegistryContainer;

  private AdminClient kafkaAdminClient;

  private KafkaConsumer<String, String> kafkaStringConsumer;

  private KafkaConsumer<String, GenericRecord> kafkaAvroConsumer;

  private KafkaProducer<String, String> kafkaStringProducer;

  private KafkaProducer<String, GenericRecord> kafkaAvroProducer;

  @Override
  public void beforeAll(ExtensionContext context) {

    Network network = Network.SHARED;

    kafkaContainer =
        new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"))
            .withEmbeddedZookeeper()
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093 ,BROKER://0.0.0.0:9092")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
            .withEnv("KAFKA_BROKER_ID", "1")
            .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .withEnv("KAFKA_LOG_FLUSH_INTERVAL_MESSAGES", Long.MAX_VALUE + "")
            .withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
            .withNetwork(network)
            .withNetworkAliases("broker")
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(10)));

    kafkaContainer.start();

    schemaRegistryContainer =
        new GenericContainer("confluentinc/cp-schema-registry:7.2.2")
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "broker:9092")
            .withExposedPorts(8081)
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .waitingFor(Wait.forListeningPort())
            .dependsOn(kafkaContainer);
    schemaRegistryContainer.setPortBindings(List.of("28081:8081"));
    schemaRegistryContainer.start();

    String schemaRegistryUrl = "http://localhost:28081";

    Properties adminProperties = new Properties();
    adminProperties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    kafkaAdminClient = AdminClient.create(adminProperties);

    Properties consumerProperties = new Properties();
    consumerProperties.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    consumerProperties.put("schema.registry.url", schemaRegistryUrl);
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    kafkaStringConsumer = new KafkaConsumer<>(consumerProperties);
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    kafkaAvroConsumer = new KafkaConsumer<>(consumerProperties);

    Properties producerProperties = new Properties();
    producerProperties.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    producerProperties.put("schema.registry.url", schemaRegistryUrl);
    producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaStringProducer = new KafkaProducer<>(producerProperties);
    producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    kafkaAvroProducer = new KafkaProducer<>(producerProperties);
  }

  @Override
  public void afterAll(ExtensionContext context) {
    kafkaStringConsumer.close();
    kafkaAvroConsumer.close();
    kafkaStringProducer.close();
    kafkaAvroProducer.close();
    kafkaContainer.stop();
  }

  @Override
  public void beforeEach(ExtensionContext context) throws IllegalAccessException {
    patchField(context, KafkaContainerField.class, kafkaContainer);
    patchField(context, KafkaAdminClientField.class, kafkaAdminClient);
    patchField(context, KafkaStringConsumerField.class, kafkaStringConsumer);
    patchField(context, KafkaAvroConsumerField.class, kafkaAvroConsumer);
    patchField(context, KafkaStringProducerField.class, kafkaStringProducer);
    patchField(context, KafkaAvroProducerField.class, kafkaAvroProducer);
  }

  @Override
  public void afterEach(ExtensionContext context) throws ExecutionException, InterruptedException {
    ListTopicsResult listTopicsResult = kafkaAdminClient.listTopics();
    kafkaAdminClient.deleteTopics(listTopicsResult.names().get()).all().get();
  }
}
