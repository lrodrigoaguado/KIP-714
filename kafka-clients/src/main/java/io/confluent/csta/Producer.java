/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.confluent.csta;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

    private static final String DEFAULT_TOPIC = "test-queues";
    private static final String BOOTSTRAP_SERVERS = "localhost:19092,localhost:29092,localhost:39092";
    private static final String DEFAULT_CLIENT_ID = "kip-714-producer-demo";

    public static void main(String[] args) throws Exception {
        String topic = args.length > 0 ? args[0] : DEFAULT_TOPIC;
        String clientId = args.length > 1 ? args[1] : DEFAULT_CLIENT_ID;

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.setProperty("client.id", clientId);

        System.out.println("Starting Producer with client.id: " + clientId);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
                new StringSerializer())) {
            produceRecords(producer, topic, Integer.MAX_VALUE, () -> {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
        }
    }

    public static void produceRecords(org.apache.kafka.clients.producer.Producer<String, String> producer,
            String topic, int recordCount, Runnable postSendAction) {

        Random random = new Random();
        int count = 0;

        while (count < recordCount) {
            try {
                String randomString = generateRandomString(random);
                int partitionNumber = random.nextInt(2);

                Future<RecordMetadata> future = producer.send(new ProducerRecord<String, String>(topic,
                        partitionNumber, null, randomString));
                RecordMetadata metadata = future.get(); // Wait for the result (synchronous)
                System.out.printf("Published record: %s into partition: %d, offset: %d\n", randomString,
                        metadata.partition(), metadata.offset());

                if (postSendAction != null) {
                    postSendAction.run();
                }
                count++;
            } catch (Exception e) {
                // Handle the exception if the thread is interrupted
                e.printStackTrace();
                break;
            }
        }
    }

    private static String generateRandomString(Random random) {
        int length = 5 + random.nextInt(16); // 5 to 20 (5 + 0..15 = 5..20 is wrong, 5+15=20. wait. nextInt(16) is
                                             // 0..15. 5+0=5. 5+15=20. Correct.)
        String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(random.nextInt(characters.length())));
        }
        return sb.toString();
    }
}
