package sustain.synopsis.ingestion.client.core;

import org.apache.log4j.Logger;
import sustain.synopsis.common.Strand;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Registry to keep track of strands at the client's end.
 * For each observation the {@link Driver} will generate a {@link Strand}, and pass it to the registry.
 * Registry will look for a similar strand using the key of the strand - if there is a match, it will merge
 * the newly added strand with the existing strand in the registry. If not the new strand will be added to the
 * registry. For each newly added strand, the registry will check for completed strands by doing a prefix
 * lookup for the geohash of the new strand.
 * For the prefix lookup, it will use the geohash of the newly added strand + its starting timestamp (which is the
 * ending timestamp of the completed strand).
 *
 * {@link Driver} will notify the registry at the end of an ingestion session. This is to make sure all strands
 * that are not yet published should be published to the registry.
 * A single writer thread model is assumed to maintain the temporal ordering between strands. The temporal ordering
 * is important to detect (with high confidence) the temporal bound of a strand.
 */
public class StrandRegistry {
    private final Logger logger = Logger.getLogger(StrandRegistry.class);
    private final StrandPublisher publisher;
    private final Map<String, Strand> registry = new HashMap<>();
    private int totalPublishedStrandCount = 0;

    public StrandRegistry(StrandPublisher publisher) {
        this.publisher = publisher;
    }

    /**
     * Adds a new strand to the registry. Merge if there is a matching strand.
     * @param strand New strand to be added
     * @return Current number of strands in the registry
     */
    public int add(Strand strand) {
        Strand existing = registry.putIfAbsent(strand.getKey(), strand);
        if (existing != null) {
            existing.merge(strand);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[" + Thread.currentThread().getName() + "] Strand is added. Key: " + strand.getKey() +
                    ", " + "Registry size: " + registry.size() + ", " + "Merged: " + (existing == null ? "false" :
                    "true"));
        }
        // publish all completed strands - it is possible for the incomplete strands to get published at this stage.
        // but as long as they are sharing the same ingestion session id, they will get merged.
//        publishStrandsWithPrefix(strand.getGeohash(), strand.getFromTimeStamp());
        return registry.size();
    }

    /**
     * Publish all the strands in the registry which has the same geo hash and a toTimeStamp <= fromTimeStamp
     * provided. It is assumed that this strands are complete, therefore published to the cloud.
     * @param geohash Geohash
     * @param fromTimestamp starting time stamp of the current strand
     */
    private void publishStrandsWithPrefix(String geohash, long fromTimestamp) {
        Map<String, Strand> strandsForPublishing = filterStrandsWithPrefix(registry, geohash, fromTimestamp);
        if (strandsForPublishing.isEmpty()) {
            return;
        }
        publisher.publish(new HashSet<>(strandsForPublishing.values()));
        strandsForPublishing.forEach((k, v) -> registry.remove(k));
        if (logger.isDebugEnabled()) {
            logger.debug("[" + Thread.currentThread().getName() + "] Strands with geohash: " + geohash +
                    ", starting" + " time stamp: " + fromTimestamp + " are " +
                    "published. Strand count: " + strandsForPublishing.size());
        }
        totalPublishedStrandCount += strandsForPublishing.size();
    }

    /**
     * Find all the strands with in the registry which has the same geo hash and a toTimeStamp <= fromTimeStamp
     * provided.
     * @param registry Current registry - this argument is introduced for unit testing
     * @param geohash Geohash prefix
     * @param fromTimestamp upper timestamp
     * @return List of strands that are likely to be complete
     */
    Map<String, Strand> filterStrandsWithPrefix(Map<String, Strand> registry, String geohash, long fromTimestamp) {
        return registry.keySet().stream().filter(k -> k.startsWith(geohash + "," + fromTimestamp)).collect(
                Collectors.toMap(k -> k, registry::get));
    }

    /**
     * Terminate the current ingestion session. Send all the remaining strands in the registry to the cloud.
     * @return Total number of strands ingested during the session.
     */
    public int terminateSession() {
        publisher.publish(new HashSet<>(registry.values()));
        totalPublishedStrandCount += registry.size();
        logger.info("[" + Thread.currentThread().getName() + "] Publishing all strands. Published strand count: " +
                registry.size());
        return totalPublishedStrandCount;
    }
}
