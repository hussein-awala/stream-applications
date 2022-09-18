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

public class MessagesFaker {
    public static void main(String[] args) throws IOException, InterruptedException {
        int waitSeconds = 5;
        int batchSize = 100;
        int nbBatchs = 1;

        String topicName = "test-topic";

        MessagesFaker messagesFaker = new MessagesFaker();
        ClassLoader classLoader = messagesFaker.getClass().getClassLoader();
        InputStream is = classLoader.getResourceAsStream("test-topic.avsc");

        Schema schema = new Schema.Parser().parse(is);

        Properties config = new Properties();
        config.put("client.id", "faker");
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        config.put("acks", "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        config.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(config);

        Random rn = new Random();

        for (int i = 0; i < nbBatchs; i++) {
            System.out.printf("Batch: %s%n", i);
            for (Object value : new RandomData(schema, batchSize)) {
                final ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topicName, String.valueOf(rn.nextInt(1000)), (GenericRecord) value);
                producer.send(record);
            }
            Thread.sleep(waitSeconds * 1000);
        }
    }
}
