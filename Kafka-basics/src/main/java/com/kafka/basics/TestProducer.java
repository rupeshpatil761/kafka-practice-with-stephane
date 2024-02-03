package com.kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestProducer {

    private static final Logger log = LoggerFactory.getLogger(TestProducer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("TestProducer main method");

        // create producer properties
        Properties props = new Properties();

        // connect to local kafka broker
        String localServerIp = "localhost:19092";
        props.setProperty("bootstrap.servers",localServerIp);

        // connect to conductor or upstash playground

        // set producer properties
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create producer record
        String topicName = "first_topic";
        String value = "First Message from Java Producer";
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName, value);

        // send data / messages
        producer.send(producerRecord);

        // flush and close the producer
        producer.flush();
        producer.close();

        // verify by consuming data on given topic either from conductor or CLI
    }
}
