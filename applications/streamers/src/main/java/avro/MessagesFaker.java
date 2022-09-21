package avro;

public class MessagesFaker {
    public static void main(String[] args) {
        MessageFakerRunnable faker = new MessageFakerRunnable( "test-topic", 5, 100, 10,  "test-topic.avsc");
        faker.run(); // no need to create a new thread
    }
}
