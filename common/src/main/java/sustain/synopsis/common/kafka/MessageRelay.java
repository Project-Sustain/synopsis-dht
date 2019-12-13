package sustain.synopsis.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface MessageRelay<K,V> {
    public void put(ConsumerRecord<K,V> record);

    public ConsumerRecord<K,V> get();
}
