package utils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.platform.commons.support.AnnotationSupport;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ser.std.StringSerializer;
import org.testcontainers.utility.DockerImageName;

public class KafkaTestExtension implements BeforeEachCallback, AfterEachCallback {

  private KafkaContainer kafkaContainer;

  private KafkaConsumer kafkaConsumer;

  private KafkaProducer kafkaProducer;

  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.2.2"));

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

    setFieldsTo(context);
  }

  @Override
  public void afterEach(ExtensionContext context) {
    kafkaContainer.stop();
    kafkaConsumer.close();
    kafkaProducer.close();
  }

  private void setFieldsTo(ExtensionContext extensionContext) throws IllegalAccessException {
    Object testInstance = extensionContext.getRequiredTestInstance();
    List<Field> fieldsToInject =
        AnnotationSupport.findAnnotatedFields(
            extensionContext.getRequiredTestClass(), KafkaContainerField.class);
    for (Field field : fieldsToInject) {
      field.set(testInstance, kafkaContainer);
    }

    fieldsToInject =
        AnnotationSupport.findAnnotatedFields(
            extensionContext.getRequiredTestClass(), KafkaConsumerField.class);
    for (Field field : fieldsToInject) {
      field.set(testInstance, kafkaConsumer);
    }

    fieldsToInject =
        AnnotationSupport.findAnnotatedFields(
            extensionContext.getRequiredTestClass(), KafkaProducerField.class);
    for (Field field : fieldsToInject) {
      field.set(testInstance, kafkaProducer);
    }
  }
}
