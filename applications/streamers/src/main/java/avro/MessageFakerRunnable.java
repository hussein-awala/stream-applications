package avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.RandomData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
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

    private Schema loadSchemaFromPath() throws ClassNotFoundException, IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        InputStream is = classLoader.getResourceAsStream(this.schemaPath);
        Schema schema = new Schema.Parser().parse(is);
        return schema;
    }

    public void run() {
        try {
            Schema schema = this.schema;
            if (schema == null) {
                if (this.schemaPath != null) {
                    schema = this.loadSchemaFromPath();
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
                for (Object value : new RandomData(schema, this.batchSize)) {
                    final ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(this.topicName, String.valueOf(rn.nextInt(1000)), (GenericRecord) value);
                    producer.send(record);
                }
                Thread.sleep(waitSeconds * 1000);
            }
        } catch (ClassNotFoundException | IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
