package sustain.synopsis.dht.store;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * Allocates available storage directories in a round robin manner to distribute the storage load
 * evenly across all directories.
 * Even though the requested capacity is not considered, it will be the same for most complete SSTables.
 * Therefore, this provides an approximately even distribution of storage.
 */
public class RoundRobinAllocationPolicy implements AllocationPolicy {

    private static final Logger logger = Logger.getLogger(RoundRobinAllocationPolicy.class);
    private int lastUsedIndex = 0;

    @Override
    public DiskManager.StorageDirectory select(int requestedCapacity, List<DiskManager.StorageDirectory> dirs) {
        for (int i = 0; i < dirs.size(); i++) {
            DiskManager.StorageDirectory next = dirs.get((lastUsedIndex++) % dirs.size());
            if (next.allocate(requestedCapacity)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Chose path: " + next.path + ", requested space: " + requestedCapacity + ", " +
                            "available space: " + next.availableSpace);
                }
                return next;
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Skipped path: " + next.path + ", requested space: " + requestedCapacity + ", " +
                        "available space: " + next.availableSpace);
            }
        }
        return null;
    }
}
