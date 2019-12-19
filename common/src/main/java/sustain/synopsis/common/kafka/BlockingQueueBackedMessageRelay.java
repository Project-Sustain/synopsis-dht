package sustain.synopsis.common.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingQueueBackedMessageRelay<K, V> implements MessageRelay<K, V> {

    private final BlockingQueue<ConsumerRecord<K, V>> queue;
    private final int readTimeout;
    private final int writeTimeout;
    private final TimeUnit timeUnit;

    public BlockingQueueBackedMessageRelay(int capacity, int readTimeout, int writeTimeout, TimeUnit tu) {
        queue = new LinkedBlockingQueue<>(capacity);
        this.readTimeout = readTimeout;
        this.writeTimeout = writeTimeout;
        this.timeUnit = tu;
    }

    @Override
    public void put(ConsumerRecord<K, V> record) {
        try {
            queue.offer(record, writeTimeout, timeUnit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ConsumerRecord<K, V> get() {
        try {
            return queue.poll(readTimeout, timeUnit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }
}
