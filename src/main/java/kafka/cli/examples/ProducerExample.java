package kafka.cli.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;

public class ProducerExample {

    public static void main(final String[] args) throws IOException {
        // Load producer configuration settings from a local file
        final Properties props = loadConfig();
        final String topic = "purchases";

        String[] users = {"eabara", "jsmith", "sgarcia", "jbernard", "htanaka", "awalther"};
        String[] items = {"book", "alarm clock", "t-shirts", "gift card", "batteries"};
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final long numMessages = 10;
            for (long i = 0L; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)] + i;

                // send with key + handle broker reply
                producer.send(
                        new ProducerRecord<>(topic, user, item + 0),
                        (event, ex) -> {
                            if (ex != null)
                                handleException(ex);
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user, item);
                        });

                // send without key
                producer.send(
                        new ProducerRecord<>(topic, item + 1),
                        (event, ex) -> {
                            if (ex != null)
                                handleException(ex);
                            else
                                System.out.printf("Produced event to topic %s: value = %s%n", topic, item);
                        });

                // fire and forget
                producer.send(new ProducerRecord<>(topic, item + 2));
            }
        }

        // send with custom partitioner
        props.setProperty("partitioner.class", PartitionerExample.class.getName());
        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final long numMessages = 10;
            for (long i = 0L; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)] + (i + 10);

                producer.send(
                        new ProducerRecord<>(topic, user, item),
                        (event, ex) -> {
                            if (ex != null)
                                handleException(ex);
                            else
                                System.out.printf("Produced event to topic %s: key = %-10s value = %s%n", topic, user, item);
                        });
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }
    }

    // We'll reuse this function to load properties from the Consumer as well
    public static Properties loadConfig() throws IOException {
        if (!Files.exists(Paths.get("src/main/resources/application.properties"))) {
            throw new IOException("src/main/resources/application.properties" + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream("src/main/resources/application.properties")) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    @SuppressWarnings("CallToPrintStackTrace")
    private static void handleException(final Throwable ex) {
        ex.printStackTrace();
    }

}

