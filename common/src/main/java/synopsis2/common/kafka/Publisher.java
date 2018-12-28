package synopsis2.common.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.util.Properties;

public class Publisher<K,V> {
    private final Logger logger = Logger.getLogger(Publisher.class);

    private final Producer<K, V> producer;

    public Publisher(Properties configProperties) {
        producer = new KafkaProducer<>(configProperties);
    }

    public void publish(String topic, K key, V value){
        ProducerRecord<K, V> rec = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(rec); // we use the fire and forget style publishing for the moment
        } catch (Exception e){
            logger.error("Error publishing the message.", e);
        }
    }
}
