package com.kafka.basics;

import org.apache.kafka.clients.producer.*;
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

        // ************** additional properties -- not recommended in prod
        //props.setProperty("batch.size", "400"); // default kafka batch size is 16kb
        //props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName()); // this is not recommended in PROD

        /*
         *
         * ## Default Behaviour of Producer with partitioner::
         * When we run below program, By default all the messages will goto same partitioner (even we have more than 1 partitioner for a topic)
         * This happens because kafka is smart enough and it applies batching on messages sent very quickly for performance improvement.
         * This is called as sticky partitioner.
         * To check, which partitioner kafka is using -- goto logs and check kafka props
         * partitioner.class=null
         */

        /*
         * To change the behaviour of partition,
         * We will be setting property - batch.size=400 (to smaller batch size) OR change the partitoner class to Round Robin
         * we will send the messages in different batches by putting outer for loop and putting sleep in between each run as below
         */

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);


        String topicName = "first_topic"; // by default creates new topic if not present
        String msg = "Message from Java Producer with callback";

        for (int i=0; i<5; i++) {

            for (int j = 0; j < 20; j++) {

                // create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, msg + " - " + j);

                // send data / messages
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // executes every time a record successfully sent or an exception is thrown
                        if (e == null) {
                            // the record sent successfully
                            log.info("Received new metadata \n" +
                                    "Topic: " + metadata.topic() + "\n" +
                                    "Partition: " + metadata.partition() + "\n" +
                                    "Offset: " + metadata.offset() + "\n" +
                                    "Timestamp: " + metadata.timestamp() + "\n");
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            log.info("------------Run : "+i+" completed ------------");
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                log.error("Sleep failed", e);
            }
        }

        // flush and close the producer
        producer.flush();
        producer.close();

        // verify by consuming data on given topic either from conductor or CLI
    }
}
