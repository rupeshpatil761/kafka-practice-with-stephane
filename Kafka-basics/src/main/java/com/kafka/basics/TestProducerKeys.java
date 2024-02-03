package com.kafka.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestProducerKeys {

    private static final Logger log = LoggerFactory.getLogger(TestProducerKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("TestProducerKeys main method");

        // create producer properties
        Properties props = new Properties();

        // connect to local kafka broker
        String localServerIp = "localhost:19092";
        props.setProperty("bootstrap.servers", localServerIp);

        // connect to conductor or upstash playground

        // set producer properties
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create producer record
        String topicName = "first_topic";

        for (int i = 0; i < 2; i++) {
            for (int j = 1; j <= 10; j++) {
                String key = "id_" + j;
                String value = "hello world " + j;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, value);
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record sent successfully
                            log.info("key: " +key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }
            log.info("------------Run : " + i + " completed ------------");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Sleep failed", e);
            }
        }

    // flush and close the producer
        producer.flush();
        producer.close();

    // Test: Observe the behaviour on console or UI, you can see the messages are with same id's are going to exact same partitions of topic
}
}
