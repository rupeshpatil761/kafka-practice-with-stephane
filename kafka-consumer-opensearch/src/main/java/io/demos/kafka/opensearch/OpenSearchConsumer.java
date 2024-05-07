package io.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getSimpleName());

    public static void main(String[] args) throws Exception {

        String topic = "wikimedia.recentchange";

        // first create an opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create our kafka client
        KafkaConsumer<String, String> consumer = createKafkaConsumer();

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

        // we need to create the index on OpenSearch if it does not exist already
        try (openSearchClient; consumer) {
            String indexName = "wikimedia";
            boolean isIndexExit = openSearchClient.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);

            if (!isIndexExit) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The "+indexName+" Index has been created");
            } else {
                log.info("The "+indexName+" Index is already exist");
            }

            // subscribe the consumer
            consumer.subscribe(Arrays.asList(topic));

            while (true) {
                // consume from kafka topic
               ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
               int recordCount = records.count();
               log.info("Received "+recordCount+" records");

               for (ConsumerRecord<String, String> record: records) {
                   // send the record into opensearch

                   // Make our consumer Idempotent
                   // strategy 1
                   // define an ID using kafka Record coordinates
                   // String recordId = record.topic()+"_"+record.partition()+"_"+record.offset();

                   try {
                       // Make our consumer Idempotent
                       // strategy 2
                       // we extract the ID from the JSON value
                       String id = extractId(record.value());

                       IndexRequest indexRequest = new IndexRequest("wikimedia")
                               .source(record.value(), XContentType.JSON)
                               .id(id); // strategy 1 -- set an ID

                       IndexResponse response = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                       log.info("Opensearch Response id: " + response.getId());
                   } catch(Exception e){
                       // ignore
                   }
               }
            }
        }
        // close things
    }

    private static String extractId(String json){
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        String groupId = "consumer-opensearch-demo";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // latest

        // create consumer
        return new KafkaConsumer<>(properties);
    }

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
        // below is for bonsai cluster
        //String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }
}
