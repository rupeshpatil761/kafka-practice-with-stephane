package io.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());
    public static void main(String... args){

        log.info("WikimediaChangesProducer main method");

        // create producer properties
        Properties props = new Properties();

        // connect to local kafka broker
        String bootstrapServers = "localhost:19092";
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // set safe producer configs (kafka <=2.8)
        // NO NEED TO SET THESE as we are using kafka > 3.0
        //props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        //props.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        //props.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        // set high throughput producer configs
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");



        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeEventHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource  = builder.build();

        // start the producer in another thread
        eventSource.start();

        // stop the producing after 10 mins
        try {
            TimeUnit.MINUTES.sleep(2);
        } catch (InterruptedException e) {
        }
        System.exit(0);
    }
}
