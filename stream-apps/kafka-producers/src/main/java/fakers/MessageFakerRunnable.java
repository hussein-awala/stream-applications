package fakers;

import io.confluent.avro.random.generator.Generator;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;
import java.util.Random;

public class MessageFakerRunnable implements Runnable {

    private String topicName;
    private int waitSeconds;
    private int batchSize;
    private int nbBatchs;
    private String schemaPath;

    private Schema schema;

    public MessageFakerRunnable(String topicName, int waitSeconds, int batchSize, int nbBatchs, String schemaPath) {
        this.topicName = topicName;
        this.waitSeconds = waitSeconds;
        this.batchSize = batchSize;
        this.nbBatchs = nbBatchs;
        this.schemaPath = schemaPath;
    }

    public MessageFakerRunnable(String topicName, int waitSeconds, int batchSize, int nbBatchs, Schema schema) {
        this.topicName = topicName;
        this.waitSeconds = waitSeconds;
        this.batchSize = batchSize;
        this.nbBatchs = nbBatchs;
        this.schema = schema;
    }

    private File loadSchemaFile() throws URISyntaxException {
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(classLoader.getResource(this.schemaPath).toURI());
    }

    public void run() {
        try {
            Generator messagesGenerator;
            if (this.schema != null) {
                messagesGenerator = new Generator.Builder().schema(this.schema).build();
            } else {
                if (this.schemaPath != null) {
                    messagesGenerator = new Generator.Builder().schemaFile(this.loadSchemaFile()).build();
                } else {
                    throw new RuntimeException("One of schema or schema file path should be provided");
                }
            }
            Properties config = new Properties();
            config.put("client.id", "faker");
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
            config.put("acks", "all");
            config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
            config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
            config.put("schema.registry.url", "http://localhost:8081");
            KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(config);
            Random rn = new Random();
            for (int i = 0; i < this.nbBatchs; i++) {
                System.out.printf("Topic %s, batch: %s%n", this.topicName, i);
                messagesGenerator.generate();
                for (int j = 0; j < this.batchSize; j++) {
                    Object message = messagesGenerator.generate();
                    final ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(this.topicName, String.valueOf(rn.nextInt(1000)), (GenericRecord) message);
                    producer.send(record);
                }
                Thread.sleep(waitSeconds * 1000);
            }
        } catch (IOException | InterruptedException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
