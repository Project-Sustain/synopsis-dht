package synopsis2.client;

import org.apache.log4j.Logger;
import synopsis2.Strand;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry to keep track of strands at the client's end.
 * This implementation is not thread-safe.
 *
 */
public class StrandRegistry {
    private final Logger logger = Logger.getLogger(StrandRegistry.class);

    private Map<String, Strand> registry = new HashMap<>();

    public int add(Strand strand){
        Strand existing = registry.putIfAbsent(strand.getKey(), strand);
        if(existing != null){
            existing.merge(strand);
            System.out.println("Strands were successfully merged.");
        }
        return registry.size();
    }
}
