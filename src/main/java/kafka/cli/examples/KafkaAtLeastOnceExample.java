package kafka.cli.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;

public class KafkaAtLeastOnceExample {

    private static final String BOOTSTRAP_SERVERS = "localhost:29091";
    private static final String TOPIC = "my-topic";
    private static final int MESSAGE_COUNT = 10;

    public static void main(String[] args) {
        // Создаем и настраиваем свойства для продюсера Kafka
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Создаем продюсера Kafka
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Отправляем сообщения продюсером
        Queue<String> messages = new LinkedList<>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = "Сообщение " + i;
            messages.add(message);
        }
        while (!messages.isEmpty()) {
            String message = messages.poll();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);
            producer.send(
                    record,
                    (event, ex) -> {
                        if (ex != null) {
                            ex.printStackTrace();
                            messages.add(event.toString());
                        } else {
                            System.out.println("Отправлено сообщение: " + message);
                        }
                    });
        }

        producer.flush();
        producer.close();

        // Создаем и настраиваем свойства для консьюмера Kafka
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // Создаем консьюмера Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);

        // Подписываемся на топик

        // В бесконечном цикле читаем сообщения из топика и выводим их
        try (consumer) {
            consumer.subscribe(Collections.singleton(TOPIC));
            ConsumerRecords<String, String> records;
            for (int i = 0; i < MESSAGE_COUNT; i += records.count()) {
                records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Получено сообщение: " + record.value());
                }
                consumer.commitSync();
            }
        }
    }
}