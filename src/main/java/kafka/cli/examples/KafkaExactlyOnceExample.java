package kafka.cli.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Properties;

public class KafkaExactlyOnceExample {

    private static final String INPUT_TOPIC = "input-topic";
    private static final String OUTPUT_TOPIC = "output-topic";
    private static final int MESSAGE_COUNT = 10;

    public static void main(String[] args) {
        // Создаем и настраиваем свойства для Kafka Streams
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29091");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        config.put(ProducerConfig.ACKS_CONFIG, "all");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "my-group");

        // Создаем топологию для Kafka Streams
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC);
        KStream<String, String> outputStream = inputStream.mapValues(value -> {
            // ... обработка сообщения ...
            return value + ", дата и время: " + OffsetDateTime.now();
        });
        outputStream.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        // Запускаем Kafka Streams
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // Остановка Kafka Streams при получении сигнала о завершении работы
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Создаем продюсера Kafka
        KafkaProducer<String, String> producer = new KafkaProducer<>(config, new StringSerializer(), new StringSerializer());

        // Отправляем сообщения продюсером
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            String message = "Сообщение " + i;
            ProducerRecord<String, String> record = new ProducerRecord<>(INPUT_TOPIC, message);
            producer.send(
                    record,
                    (event, ex) -> {
                        if (ex != null) {
                            ex.printStackTrace();
                        } else {
                            System.out.println("Отправлено сообщение: " + message);
                        }
                    });
        }

        producer.flush();
        producer.close();

        // Создаем консьюмера Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer());

        // Подписываемся на топик

        // В бесконечном цикле читаем сообщения из топика и выводим их
        try (consumer) {
            consumer.subscribe(Collections.singleton(OUTPUT_TOPIC));
            ConsumerRecords<String, String> records;
            for (int i = 0; i < MESSAGE_COUNT; i += records.count()) {
                records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Получено сообщение: " + record.value());
                }
            }
        }

        streams.close();
    }
}
