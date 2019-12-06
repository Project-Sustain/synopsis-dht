package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.dispersion.DataDispersionScheme;
import sustain.synopsis.dht.dispersion.DataDispersionSchemeFactory;
import sustain.synopsis.dht.zk.MembershipListener;
import sustain.synopsis.dht.zk.MembershipTracker;
import sustain.synopsis.dht.zk.ZKError;

import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Consistent Hashing implementation is inspired by http://www.tom-e-white.com/2007/11/consistent-hashing.html
 *
 * @author Thilina Buddhika
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
            return "Entity{" +
                    "id=" + id +
                    ", addr='" + addr + '\'' +
                    ", virtualId=" + virtualId +
                    '}';
        }
    }

    private Queue<List<String>> updates = new LinkedBlockingDeque<>();
    private final SortedMap<BigInteger, Entity> spatialRing = new TreeMap<>(); // outer ring with spatial data
    private List<String> temporalNodes = new ArrayList<>();
    private final Map<String, BigInteger> dataAddressToIdMapping = new HashMap<>();
    private DataDispersionScheme dataDispersionScheme;

    public Ring() {
        dataDispersionScheme = DataDispersionSchemeFactory.getInstance().getDataDispersionScheme(
                ServerConstants.DATA_DISPERSION_SCHEME.CONSISTENT_HASHING);
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
                synchronized (this) {
                    if (updates.peek() == null) {
                        if (updates.peek() == null) {
                            try {
                                this.wait();
                            } catch (InterruptedException e) {
                                logger.error("Ring updater thread interrupted waiting for updates.", e);
                            }
                        }
                    }
                    if (!updates.isEmpty()) {
                        List<String> newNodes = updates.remove();
                        updateRings(newNodes);
                    }
                }
            } catch (Throwable e) {
                logger.error("Error in ring updater thread.", e);
            }
        }
    }

    @Override
    public synchronized void handleMembershipChange(List<String> update) {
        boolean isEmpty = (updates.size() == 0);
        updates.add(update);
        if (isEmpty) {
            this.notifyAll();
        }
    }

    public synchronized void updateRings(List<String> nodes) {
        updateRing(nodes, dataDispersionScheme, spatialRing, dataAddressToIdMapping);
        logger.info("Updated data ring structure for new nodes. New node count: " + nodes.size());
    }

    private void updateRing(List<String> nodes, DataDispersionScheme dispersionScheme,
                            SortedMap<BigInteger, Entity> circle, Map<String, BigInteger> addressMapping) {
        List<Entity> entities = dispersionScheme.processNewMembers(nodes);
        if (!dispersionScheme.incrementalUpdatesToMembers()) {
            circle.clear();
            addressMapping.clear();
        }
        for (Entity entity : entities) {
            circle.put(entity.id, entity);
            addressMapping.put(entity.addr + ":" + entity.virtualId, entity.id);
        }
    }

    public String lookup(String entityId) {
        return search(dataDispersionScheme.getIdentifier(entityId), spatialRing);
    }

    public BigInteger getIdentifier(String key) {
        return dataDispersionScheme.getIdentifier(key);
    }

    private String search(BigInteger identifier, SortedMap<BigInteger, Entity> circle) {
        if (!circle.containsKey(identifier)) {
            SortedMap<BigInteger, Entity> tailMap = circle.tailMap(identifier);
            identifier = tailMap.isEmpty() ? circle.firstKey() : tailMap.firstKey();
        }
        return circle.get(identifier).addr;
    }
}
