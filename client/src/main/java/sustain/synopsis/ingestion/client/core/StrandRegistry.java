package sustain.synopsis.ingestion.client.core;

import org.apache.log4j.Logger;
import sustain.synopsis.common.Strand;

import java.util.*;

/**
 * Registry to keep track of strands at the client's end.
 * For each observation the {@link StrandConversionTaskManager} will generate a {@link Strand}, and pass it to the
 * registry.
 * Registry will look for a similar strand using the key of the strand - if there is a match, it will merge
 * the newly added strand with the existing strand in the registry. If not the new strand will be added to the
 * registry. For each newly added strand, the registry will check for completed strands by doing a prefix
 * lookup for the geohash of the new strand.
 * For the prefix lookup, it will use the geohash of the newly added strand + its starting timestamp (which is the
 * ending timestamp of the completed strand).
 * <p>
 * {@link StrandConversionTaskManager} will notify the registry at the end of an ingestion session. This is to make
 * sure all strands
 * that are not yet published should be published to the registry.
 * A single writer thread model is assumed to maintain the temporal ordering between strands. The temporal ordering
 * is important to detect (with high confidence) the temporal bound of a strand.
 */
public class StrandRegistry {
    private final Logger logger = Logger.getLogger(StrandRegistry.class);
    private final StrandPublisher publisher;
    private final Map<String, Strand> strandKeyMap = new HashMap<>();
    private final LRUCache<Strand> lruCache = new LRUCache<>();
    private final int cacheSize;
    private final int publishBatchSize;

    private long totalPublishedStrandCount = 0;

    public StrandRegistry(StrandPublisher publisher) {
        this.publisher = publisher;
        this.cacheSize = 10000;
        this.publishBatchSize = 100;
    }

    public StrandRegistry(StrandPublisher publisher, int cacheSize, int publishBatchSize) {
        this.publisher = publisher;
        this.cacheSize = cacheSize;
        this.publishBatchSize = publishBatchSize;
    }

    /**
     * Adds a new strand to the registry. Merge if there is a matching strand.
     * @param strand New strand to be added
     * @return Current number of strands in the registry
     */
    public long add(Strand strand) {
        Strand existing = strandKeyMap.get(strand.getKey());
        if (existing != null) {
            existing.merge(strand);
            lruCache.use(existing);
        } else {
            strandKeyMap.put(strand.getKey(), strand);
            lruCache.use(strand);
        }

        if (logger.isTraceEnabled()) {
            logger.trace("[" + Thread.currentThread().getName() + "] Strand is added. Key: " + strand.getKey() +
                    ", " + "Registry size: " + strandKeyMap.size() + ", " + "Merged: " + (existing == null ? "false" :
                    "true"));
        }

        if (lruCache.size() >= cacheSize) {
            Collection<Strand> strands = lruCache.evictLRU(publishBatchSize);
            strands.forEach(s -> strandKeyMap.remove(s.getKey()));
            publish(strands);
        }
        return strandKeyMap.size();
    }

    public long getTotalPublishedStrandCount() {
        return totalPublishedStrandCount;
    }

    private void publish(Collection<Strand> strands) {
        publisher.publish(strands);
        totalPublishedStrandCount += strands.size();
    }

    /**
     * Terminate the current ingestion session. Send all the remaining strands in the registry to the cloud.
     * @return Total number of strands ingested during the session.
     */
    public long terminateSession() {
        List<Strand> strands = lruCache.evictAll();
        for (int i = 0; i < lruCache.size(); i += publishBatchSize) {
            int endIdx = Math.min(i + publishBatchSize, lruCache.size());
            publish(strands.subList(i, endIdx));
        }

        publish(lruCache.evictAll());
        publisher.terminateSession();
        logger.info("[" + Thread.currentThread().getName() + "] Publishing all strands. Published strand count: " +
                strandKeyMap.size());

        return totalPublishedStrandCount;
    }


}
