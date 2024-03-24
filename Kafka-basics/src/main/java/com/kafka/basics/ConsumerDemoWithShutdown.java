package com.kafka.basics;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {

        String topic = "first_topic";
        String localServerIp = "localhost:19092";
        String groupId= "my-java-application";

        // create consumer properties
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",localServerIp);
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest"); // none/earliest/latest
        props.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        // if we do not provide group id: We get below error
        // Error: To use the group management or offset commit APIs, you must provide a valid group.id in the consumer configuration

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // get a reference to main thread to perform graceful termination
        final Thread mainThread = Thread.currentThread();

        // adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown, lets exit by calling consumer.wakeup");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            // poll for data
            while (true) {
                log.info("polling");

                // waiting upto 1 sec to reception of the data
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Received new metadata \n" +
                            "Topic: " + record.topic() + "\n" +
                            "Partition: " + record.partition() + "\n" +
                            "Offset: " + record.offset() + "\n" +
                            "Timestamp: " + record.timestamp() + "\n");
                }
            }
        } catch(WakeupException we) {
            log.error("Consumer is starting to shut down ");
        } catch (Exception e) {
            log.error("The consumer is now gracefully shut down ");
        } finally {
            consumer.close();
            log.info("The consumer is now gracefully shut down");
        }

        // verify the consumer graceful shut down by stopping the running application
    }
}