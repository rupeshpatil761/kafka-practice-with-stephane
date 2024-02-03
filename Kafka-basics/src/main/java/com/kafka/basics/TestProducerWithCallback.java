package com.kafka.basics;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TestProducerWithCallback {

    private static final Logger log = LoggerFactory.getLogger(TestProducerWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("TestProducerWithCallback main method");

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
        String topicName = "first_topic"; // by default creates new topic if not present
        String value = "First Message from Java Producer with callback";
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topicName, value);

        // send data / messages
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                // executes every time a record successfully sent or an exception is thrown
                if (e == null) {
                   // the record sent successfully
                    log.info("Received new metadata \n"+
                            "Topic: "+metadata.topic() +"\n" +
                            "Partition: "+metadata.partition() +"\n" +
                            "Offset: "+metadata.offset() +"\n" +
                            "Timestamp: "+metadata.timestamp() +"\n");
                } else {
                    log.error("Error while producing", e);
                }
            }
        });

        // flush and close the producer
        producer.flush();
        producer.close();

        // verify by consuming data on given topic either from conductor or CLI
    }
}
