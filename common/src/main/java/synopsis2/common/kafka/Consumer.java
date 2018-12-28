package synopsis2.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class Consumer<K, V> implements Runnable {
    private final KafkaConsumer<K, V> consumer;
    private MessageRelay<K, V> relay;

    public Consumer(Properties props, String regex, MessageRelay<K, V> relay) {
        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Pattern.compile(regex));
        this.relay = relay;
    }

    public void run() {
        while (true) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<K, V> rec : records) {
                relay.put(rec);
            }
            // todo:
            // implement a throttling mechanism based on the isReady() method of the message relay
        }
    }
}
