package avro;

import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.exceptions.CsvException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Properties;

public class CsvMessages {
    public static void main(String[] args) throws IOException, CsvException {
        String topicName = "power-consumption";

        CsvMessages csvMessages = new CsvMessages();
        ClassLoader classLoader = csvMessages.getClass().getClassLoader();
        InputStream schemaInputStream = classLoader.getResourceAsStream("power_consumption.avsc");
        Schema schema = new Schema.Parser().parse(schemaInputStream);

        Properties config = new Properties();
        config.put("client.id", "faker");
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        config.put("acks", "all");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        config.put("schema.registry.url", "http://localhost:8081");

        final KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(config);

        InputStream dataInputStream = classLoader.getResourceAsStream("data/household_power_consumption.txt");
        CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build();
        try (CSVReader reader = new CSVReaderBuilder(new InputStreamReader(dataInputStream)).withCSVParser(csvParser)   // custom CSV parser
                .withSkipLines(1)           // skip the first line, header info
                .build()) {
            List<String[]> rows = reader.readAll();
            for (String[] row : rows) {
                try {
                    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema);
                    recordBuilder.set("Date", row[0]);
                    recordBuilder.set("Time", row[1]);
                    recordBuilder.set("Global_active_power", Double.parseDouble(row[2]));
                    recordBuilder.set("Global_reactive_power", Double.parseDouble(row[3]));
                    recordBuilder.set("Voltage", Double.valueOf(row[4]));
                    recordBuilder.set("Global_intensity", Double.parseDouble(row[5]));
                    recordBuilder.set("Sub_metering_1", Double.parseDouble(row[6]));
                    recordBuilder.set("Sub_metering_2", Double.parseDouble(row[7]));
                    recordBuilder.set("Sub_metering_3", Double.parseDouble(row[8]));
                    final GenericRecord genericRecord = recordBuilder.build();
                    final ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(topicName, null, genericRecord);
                    producer.send(producerRecord);
                } catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        }
    }
}
