package com.tesco.price.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class Consumer implements Runnable {
    private volatile boolean done = false;
    private KafkaConsumer<String, String> consumer;

    public Consumer() {
        Properties props = new Properties();
//        props.put("bootstrap.servers", "kafka.test-price-service.com:9092");
        props.put("bootstrap.servers", "35.158.29.172:9092");
//        props.put("bootstrap.servers", "kafka.test-price-service.com:443");

        props.put("group.id", "test-1");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='consumer' password='VugJRtLRKU3wsTQ2';");
        props.put("security.protocol", "SASL_PLAINTEXT");
//        props.put("sasl.mechanism", "SSL");

//        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");

//        props.put("security.protocol", "SSL");
//        props.put("ssl.truststore.location", "kafka.client.truststore.jks");
//        props.put("ssl.truststore.password", "test1234");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test"));
    }

    @Override
    public void run() {
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("--------------------------- START RECEIVING INVOICES -----------------------------------");
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------------------------------");

        while (!done) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("offset = %d, key = %s", record.offset(), record.key()));
                System.out.println(String.format("value = %s", record.value()));
                System.out.println("----------------------------------------------------------------------------------------");
                System.out.println("----------------------------------------------------------------------------------------");
            }
        }

        consumer.close();
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("-------------------------- FINISHED RECEIVING INVOICES ---------------------------------");
        System.out.println("----------------------------------------------------------------------------------------");
        System.out.println("----------------------------------------------------------------------------------------");
    }

    public void done() {
        done = true;
    }
}
