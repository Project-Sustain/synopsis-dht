package sustain.synopsis.dht.store;

import java.util.List;

/**
 * Defines a policy for allocating available storage directories to continuous storage requests.
 */
public interface AllocationPolicy {
    DiskManager.StorageDirectory select(int requestedCapacity, List<DiskManager.StorageDirectory> dirs);
}
