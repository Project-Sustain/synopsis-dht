package synopsis2.client.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import synopsis2.dht.Context;
import synopsis2.dht.ServerConstants;

import java.util.Properties;

public class Publisher {
    private final Logger logger = Logger.getLogger(Publisher.class);

    private final Producer<String, byte[]> producer;

    public Publisher() {
        Context ctxt = Context.getInstance();
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                ctxt.getProperty(ServerConstants.Configuration.KAFKA_BOOTSTRAP_BROKERS));
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        producer = new KafkaProducer<>(configProperties);
    }

    public void publish(String topic, String key, byte[] serializedStrand){
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>(topic, key, serializedStrand);
        try {
            producer.send(rec); // we use the fire and forget style publishing for the moment
        } catch (Exception e){
            logger.error("Error publishing the message.", e);
        }
    }
}
