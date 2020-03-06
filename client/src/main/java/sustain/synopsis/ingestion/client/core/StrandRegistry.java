package sustain.synopsis.ingestion.client.core;

import org.apache.log4j.Logger;
import sustain.synopsis.common.Strand;

import java.util.*;

/**
 * Registry to keep track of strands at the client's end.
 * For each observation the {@link IngestionTaskManager} will generate a {@link Strand}, and pass it to the registry.
 * Registry will look for a similar strand using the key of the strand - if there is a match, it will merge
 * the newly added strand with the existing strand in the registry. If not the new strand will be added to the
 * registry. For each newly added strand, the registry will check for completed strands by doing a prefix
 * lookup for the geohash of the new strand.
 * For the prefix lookup, it will use the geohash of the newly added strand + its starting timestamp (which is the
 * ending timestamp of the completed strand).
 *
 * {@link IngestionTaskManager} will notify the registry at the end of an ingestion session. This is to make sure all strands
 * that are not yet published should be published to the registry.
 * A single writer thread model is assumed to maintain the temporal ordering between strands. The temporal ordering
 * is important to detect (with high confidence) the temporal bound of a strand.
 */
public class StrandRegistry {
    private final Logger logger = Logger.getLogger(StrandRegistry.class);
    private final StrandPublisher publisher;
    private final Map<String, Strand> registry = new HashMap<>();
    private final LRUCache<Strand> lruCache = new LRUCache<>();

    private final int publishAtCacheSize = 100000;
    private final int numToPublish = 100;

    private long totalPublishedStrandCount = 0;


    public StrandRegistry(StrandPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * Adds a new strand to the registry. Merge if there is a matching strand.
     * @param strand New strand to be added
     * @return Current number of strands in the registry
     */
    public long add(Strand strand) {
        Strand existing = registry.get(strand.getKey());
        if (existing != null) {
            existing.merge(strand);
            lruCache.use(existing);
        } else {
            registry.put(strand.getKey(), strand);
            lruCache.use(strand);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("[" + Thread.currentThread().getName() + "] Strand is added. Key: " + strand.getKey() +
                    ", " + "Registry size: " + registry.size() + ", " + "Merged: " + (existing == null ? "false" :
                    "true"));
        }

        if (lruCache.size() >= publishAtCacheSize) {
            Collection<Strand> strands = lruCache.evictLRU(numToPublish);
            strands.forEach(s -> registry.remove(s.getKey()));
            publish(strands);
        }
        return registry.size();
    }

    public long getTotalPublishedStrandCount() {
        return totalPublishedStrandCount;
    }

    private void publish (Collection<Strand> strands) {
        publisher.publish(strands);
        totalPublishedStrandCount += strands.size();
    }

    /**
     * Terminate the current ingestion session. Send all the remaining strands in the registry to the cloud.
     * @return Total number of strands ingested during the session.
     */
    public long terminateSession() {
        publish(lruCache.evictAll());
        logger.info("[" + Thread.currentThread().getName() + "] Publishing all strands. Published strand count: " +
                registry.size());
        return totalPublishedStrandCount;
    }



}
