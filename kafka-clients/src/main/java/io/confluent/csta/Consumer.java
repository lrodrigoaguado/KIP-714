package io.confluent.csta;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {

    private static final String BOOTSTRAP_SERVERS = "localhost:29092,localhost:39092,localhost:49092";
    private static final String DEFAULT_GROUP_ID = "demo-group";
    private static final String TOPIC_NAME = "test-queues";

    public static void main(String[] args) {
        String groupId = args.length > 0 ? args[0] : DEFAULT_GROUP_ID;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("group.id", groupId);
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("auto.offset.reset", "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumeRecords(consumer, TOPIC_NAME, () -> !Thread.currentThread().isInterrupted());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void consumeRecords(org.apache.kafka.clients.consumer.Consumer<String, String> consumer,
            String topic, java.util.function.BooleanSupplier loopCondition) {
        consumer.subscribe(Arrays.asList(topic));
        System.out.printf("Consumer processing topic: %s%n", topic);

        while (loopCondition.getAsBoolean()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed record from partition %d at offset %d: %s%n",
                        record.partition(), record.offset(), record.value());
            }
        }
    }
}
