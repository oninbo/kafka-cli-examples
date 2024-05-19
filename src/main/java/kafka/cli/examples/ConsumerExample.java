package kafka.cli.examples;

import org.apache.kafka.clients.consumer.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerExample {

    public static void main(final String[] args) throws Exception {
        final String topic = "purchases";

        // Load consumer configuration settings from a local file
        // Reusing the loadConfig method from the ProducerExample class
        final Properties props = ProducerExample.loadConfig();

        // Add additional properties.
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-java-getting-started");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (final Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    int partition = record.partition();
                    String recordTopic = record.topic();
                    System.out.printf("Consumed event from topic %s, partition: %d: key = %-10s value = %s%n", recordTopic, partition, key, value);
                }
            }
        }
    }

}