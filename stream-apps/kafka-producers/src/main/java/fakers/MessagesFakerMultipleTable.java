package fakers;

public class MessagesFakerMultipleTable {
    public static void main(String[] args) {
        MessageFakerRunnable faker1 = new MessageFakerRunnable("table1", 10, 100, 100, "join/table1.avsc");
        MessageFakerRunnable faker2 = new MessageFakerRunnable("table2", 60, 200, 50, "join/table2.avsc");
        new Thread(faker1).start();
        new Thread(faker2).start();
    }
}
