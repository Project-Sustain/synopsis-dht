package synopsis2.client;

import io.sigpipe.sing.serialization.SerializationOutputStream;
import org.apache.log4j.Logger;
import synopsis2.Strand;
import synopsis2.common.kafka.Publisher;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry to keep track of strands at the client's end.
 * This implementation is not thread-safe.
 */
public class StrandRegistry {
    private final Logger logger = Logger.getLogger(StrandRegistry.class);

    private Map<String, Strand> registry = new HashMap<>();

    public int add(Strand strand) {
        Strand existing = registry.putIfAbsent(strand.getKey(), strand);
        if (existing != null) {
            existing.merge(strand);
            System.out.println("Strands were successfully merged.");
        }
        return registry.size();
    }

    public boolean isBatchReady() {
        return registry.size() > 50000; // send every 50k records.
    }

    public void publish(String topic, Publisher<String, byte[]> publisher) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializationOutputStream dos = new SerializationOutputStream(baos);
        Map<String, Strand> nextRegistry = new HashMap<>();
        for (String key : registry.keySet()) {
            try {
                Strand strand = registry.get(key);
                strand.serialize(dos);
                dos.flush();
                baos.flush();
                publisher.publish(topic, strand.getKey(), baos.toByteArray());
                baos.reset();
            } catch (IOException e) {
                logger.error("Error serializing the strand.", e);
                nextRegistry.put(key, registry.get(key));
            }
        }
        this.registry = nextRegistry;
        logger.info("Finished publishing the strands.");
    }
}
