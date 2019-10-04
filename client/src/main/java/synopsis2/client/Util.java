package synopsis2.client;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.log4j.Logger;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import synopsis2.dht.Context;
import synopsis2.dht.ServerConstants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Util {

    private static final Logger LOGGER = Logger.getLogger(Util.class);

    public static Map<String, Quantizer> quantizerMapFromFile(String filePath) throws IOException {
        Map<String, Quantizer> quantizers = new HashMap<>();
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bfr = new BufferedReader(fileReader);
            String line;
            while ((line = bfr.readLine()) != null) {
                String[] segments = line.split(",");
                String fName = segments[0];
                Feature[] ticks = new Feature[segments.length - 4];
                for (int i = 4; i < segments.length; i++) {
                    ticks[i - 4] = new Feature(Float.parseFloat(segments[i]));
                }
                Quantizer q = new Quantizer(ticks);
                quantizers.put(fName, q);
            }
        } catch (IOException e) {
            LOGGER.error("Error parsing the bin configuration.", e);
            throw e;
        }
        return quantizers;
    }

    public static void createKafkaTopicIfNotExists(String topic, int partitions, short replicationCount) throws Exception {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                Context.getInstance().getProperty(ServerConstants.Configuration.KAFKA_BOOTSTRAP_BROKERS));
        AdminClient admin = AdminClient.create(properties);
        try {
            // get the list of existing topics
            ListTopicsResult existingTopicsResult = admin.listTopics();
            KafkaFuture<Set<String>> topicListFuture = existingTopicsResult.names();
            Set<String> topics = topicListFuture.get();
            if(topics.contains(topic)){
                System.out.println("Topic already exists. Topic: " + topic);
                return;
            }
            // topic does not exist
            CreateTopicsResult result = admin.createTopics(Arrays.asList(new NewTopic(topic, partitions, replicationCount).configs(new HashMap<>())));
            KafkaFuture<Void> future = result.all();
            future.get();
            System.out.println("Created new topic. Topic: " + topic);
        } catch (InterruptedException | ExecutionException e) {
            throw new Exception("Error creating topic.", e);
        }
    }
}
