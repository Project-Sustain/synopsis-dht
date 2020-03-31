package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.dispersion.DataDispersionSchemeFactory;
import sustain.synopsis.dht.dispersion.RingIdMapper;
import sustain.synopsis.dht.zk.MembershipListener;
import sustain.synopsis.dht.zk.MembershipTracker;
import sustain.synopsis.dht.zk.ZKError;

import java.math.BigInteger;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Consistent Hashing implementation is inspired by http://www.tom-e-white.com/2007/11/consistent-hashing.html
 */
public class Ring implements Runnable, MembershipListener {

    private final BlockingQueue<List<String>> updates = new LinkedBlockingDeque<>();
    private final SortedMap<BigInteger, Entity> ring = new TreeMap<>();
    private final RingIdMapper ringIdMapper;
    private final MembershipTracker membershipTracker;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Logger logger = Logger.getLogger(Ring.class);

    // used for unit testing
    public Ring(RingIdMapper dispersionScheme, MembershipTracker membershipTracker) {
        this.ringIdMapper = dispersionScheme;
        this.membershipTracker = membershipTracker;
    }

    public Ring() throws ZKError {
        ringIdMapper = DataDispersionSchemeFactory.getInstance().getDataDispersionScheme(
                ServerConstants.DATA_DISPERSION_SCHEME.CONSISTENT_HASHING);
        membershipTracker = MembershipTracker.getInstance();
    }

    @Override
    public void run() {
        membershipTracker.subscribe(this);
        membershipTracker.getAvailableWorkers();
        // listen for membership changes and update the ring data structure
        logger.debug("Ring Updater Thread started.");
        while (!Thread.interrupted()) {
            try {
                List<String> newNodes = updates.take();
                if (!newNodes.isEmpty()) {
                    List<Entity> entities = newNodes.stream().map(this::convertToEntity).collect(Collectors.toList());
                    updateRing(entities);
                }
            } catch (InterruptedException e) {
                return;
            } catch (Throwable e) {
                e.printStackTrace();
                logger.error("Error in ring updater thread.", e);
            }
        }
    }

    @Override
    public void handleMembershipChange(List<String> update) {
        updates.add(update);
    }

    void updateRing(List<Entity> entities) {
        try {
            lock.writeLock().lock();
            for (Entity entity : entities) {
                ring.put(entity.id, entity);
            }
            logger.info("Updated data ring structure for new nodes. Total node count: " + ring.size());
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String lookup(String key) {
        return lookup(ringIdMapper.getIdentifier(key));
    }

    private String lookup(BigInteger identifier) {
        try {
            lock.readLock().lock();
            if (!ring.containsKey(identifier)) {
                SortedMap<BigInteger, Entity> tailMap = ring.tailMap(identifier);
                identifier = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
            }
            return ring.get(identifier).addr;
        } finally {
            lock.readLock().unlock();
        }
    }

    Entity convertToEntity(String newNodeStr) {
        // each string is of the form address:port:v_node_id
        BigInteger identifier = ringIdMapper.getIdentifier(newNodeStr);
        String[] segments = newNodeStr.split(":");
        return new Entity(identifier, segments[0] + ":" + segments[1], Integer.parseInt(segments[2]));
    }

    public long getSize() {
        try {
            lock.readLock().lock();
            return ring.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    public static class Entity implements Comparable<Entity> {
        private BigInteger id;
        private String addr;
        private int virtualId;

        public Entity(BigInteger id, String addr, int virtualId) {
            this.id = id;
            this.addr = addr;
            this.virtualId = virtualId;
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

        BigInteger getId() {
            return id;
        }

        String getAddr() {
            return addr;
        }

        int getVirtualId() {
            return virtualId;
        }
    }
}
