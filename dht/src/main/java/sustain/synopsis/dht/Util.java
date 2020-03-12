package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import sustain.synopsis.dht.store.StrandStorageKey;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.NavigableMap;

/**
 * @author Thilina Buddhika
 */
public class Util {

    private static Logger logger = Logger.getLogger(Util.class);

    public static String getHostname() {
        InetAddress inetAddr;
        try {
            inetAddr = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            inetAddr = InetAddress.getLoopbackAddress();
        }
        return inetAddr.getHostName();
    }

    public static String getNodeAddress() {
        Context ctx = Context.getInstance();
        // we use ingestion service port to construct the node address - a unique port is needed
        // if there are multiple server processes running on the same machine
        return ctx.getProperty(ServerConstants.HOSTNAME) + ":" + ctx.getNodeConfig().getIngestionServicePort();
    }

    static String createZKDirectory(ZooKeeper zk, String path, CreateMode createMode) throws KeeperException,
            InterruptedException {
        try {
            return zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
        } catch (InterruptedException | KeeperException e) {
            throw e;
        }
    }

    public static int getVirtualNodeCount() {
        double headSizeInGB =
                ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / (1024.0 * 1024 * 1024);
        int vNodeCount = 200; // 200 works well with NOAA data.
        if (headSizeInGB != -1) {
            if (headSizeInGB <= 7) {
                vNodeCount = 100;
            } else if (headSizeInGB <= 11) {
                vNodeCount = 150;
            }
        }
        logger.info("Maximum heap size: " + headSizeInGB + ", allocated virtual node count: " + vNodeCount);
        return vNodeCount;
    }

    public static int getWorkerPoolSize() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    /**
     * Filters out the {@link StrandStorageKey} objects that falls into the given temporal boundaries
     *
     * @param metadataMap       Map of {@link StrandStorageKey} objects arranged as a {@link NavigableMap}.
     * @param lowerBound        Lower bound of the temporal bracket, usually specified as an epoch, inclusive
     * @param upperBound        Upper bound of the temporal bracket, usually specified as an epoch, exclusive by default
     * @param includeUpperBound Whether to include any strand that has a starting ts equal to the upper bound
     * @param <T>               Value type of the map
     * @return Matching StrandStorageKeys and their associated values as a {@link NavigableMap}
     */
    public static <T> NavigableMap<StrandStorageKey, T> temporalLookup(NavigableMap<StrandStorageKey, T> metadataMap,
                                                                       long lowerBound, long upperBound,
                                                                       boolean includeUpperBound) {
        /* {@link StrandStorageKey} uses the 'from' attribute in the compare(). Therefore, we can use a dummy value
        as the 'to' attribute.
         */
        StrandStorageKey from = new StrandStorageKey(lowerBound, Long.MAX_VALUE);
        /* handle the case where lower bound is falling into the temporal bracket of a strand - subMap()
        skips that strand because lowerBound > strand.getStartTS(). Also make sure the strand.getEndTS() < lowerBound
        . */
        StrandStorageKey floorKey = metadataMap.floorKey(new StrandStorageKey(lowerBound, upperBound));
        if (floorKey != null && floorKey.getEndTS() > lowerBound) {
            from = floorKey;
        }

        StrandStorageKey to = new StrandStorageKey(upperBound, Long.MAX_VALUE);
        return metadataMap.subMap(from, true, to, includeUpperBound);
    }
}
