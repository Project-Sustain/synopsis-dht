package synopsis2.client.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {
    /*public static void main(String[] argv)throws Exception {
        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        org.apache.kafka.clients.producer.Producer producer = new KafkaProducer<String, String>(configProperties);
        String line = "test";
        while(!line.equals("exit")) {
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>("test", line);
            producer.send(rec);
            line = in.nextLine();
        }
        in.close();
        producer.close();
    }*/
}
