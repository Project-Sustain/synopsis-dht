package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.dispersion.DataDispersionScheme;
import sustain.synopsis.dht.dispersion.DataDispersionSchemeFactory;
import sustain.synopsis.dht.zk.MembershipListener;
import sustain.synopsis.dht.zk.MembershipTracker;
import sustain.synopsis.dht.zk.ZKError;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Consistent Hashing implementation is inspired by http://www.tom-e-white.com/2007/11/consistent-hashing.html
 */
public class Ring implements Runnable, MembershipListener {

    private Logger logger = Logger.getLogger(Ring.class);

    public static class Entity implements Comparable<Entity> {
        private BigInteger id;
        private String addr;
        private int virtualId;

        public Entity(BigInteger id, String addr, int virtualId) {
            this.id = id;
            this.addr = addr;
            this.virtualId = virtualId;
        }

        public String getAddr() {
            return addr;
        }

        public BigInteger getId() {
            return id;
        }

        @Override
        public int compareTo(Entity o) {
            return id.compareTo(o.id);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entity entity = (Entity) o;
            return id.equals(entity.id);
        }

        @Override
        public int hashCode() {
            return id.hashCode();
        }

        @Override
        public String toString() {
            return "Entity{" + "id=" + id + ", addr='" + addr + '\'' + ", virtualId=" + virtualId + '}';
        }
    }

    private BlockingQueue<List<String>> updates = new LinkedBlockingDeque<>();
    private final SortedMap<BigInteger, Entity> ring = new TreeMap<>();
    private final Map<String, BigInteger> dataAddressToIdMapping = new HashMap<>();
    private final DataDispersionScheme dataDispersionScheme =
            DataDispersionSchemeFactory.getInstance().getDataDispersionScheme(ServerConstants.DATA_DISPERSION_SCHEME.CONSISTENT_HASHING);
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public Ring() {

    }

    @Override
    public void run() {
        // subscribe for membership changes.
        try {
            MembershipTracker membershipTracker = MembershipTracker.getInstance();
            membershipTracker.subscribe(this);
            membershipTracker.getAvailableWorkers();
        } catch (ZKError zkError) {
            logger.error("Error subscribing to membership changes. Terminating the ring updater thread.", zkError);
            return;
        }

        // listen for membership changes and update the ring data structure
        logger.debug("Ring Updater Thread started.");
        while (!Thread.interrupted()) {
            try {
                List<String> newNodes = updates.take();
                if (!updates.isEmpty()) {
                    updateRing(newNodes, dataDispersionScheme, ring, dataAddressToIdMapping); // todo: remove
                    // indirection
                }
            } catch (Throwable e) {
                logger.error("Error in ring updater thread.", e);
            }
        }
    }

    @Override
    public void handleMembershipChange(List<String> update) {
        updates.add(update);
    }

    private void updateRing(List<String> nodes, DataDispersionScheme dispersionScheme,
                            SortedMap<BigInteger, Entity> circle, Map<String, BigInteger> addressMapping) {
        try {
            lock.writeLock().lock();
            List<Entity> entities = dispersionScheme.processNewMembers(nodes);
            if (!dispersionScheme.incrementalUpdatesToMembers()) {
                circle.clear();
                addressMapping.clear();
            }
            for (Entity entity : entities) {
                circle.put(entity.id, entity);
                addressMapping.put(entity.addr + ":" + entity.virtualId, entity.id);
            }
            logger.info("Updated data ring structure for new nodes. New node count: " + nodes.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String lookup(String entityId) {
        return search(dataDispersionScheme.getIdentifier(entityId), ring);
    }

    public BigInteger getIdentifier(String key) {
        return dataDispersionScheme.getIdentifier(key);
    }

    private String search(BigInteger identifier, SortedMap<BigInteger, Entity> circle) {
        try {
            lock.readLock().lock();
            if (!circle.containsKey(identifier)) {
                SortedMap<BigInteger, Entity> tailMap = circle.tailMap(identifier);
                identifier = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
            }
            return circle.get(identifier).addr;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Ring)) return false;
        Ring ring1 = (Ring) o;
        return updates.equals(ring1.updates) && ring.equals(ring1.ring) && dataAddressToIdMapping.equals(ring1.dataAddressToIdMapping) && dataDispersionScheme.equals(ring1.dataDispersionScheme);
    }

    @Override
    public int hashCode() {
        return Objects.hash(updates, ring, dataAddressToIdMapping, dataDispersionScheme);
    }
}
