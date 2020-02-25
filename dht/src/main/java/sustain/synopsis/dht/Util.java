package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author Thilina Buddhika
 */
public class Util {

    private static Logger logger = Logger.getLogger(Util.class);

    // we assume 128-bit identifier.
    public static final BigInteger BASE = BigInteger.valueOf(2).pow(128);

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
        return ctx.getProperty(ServerConstants.Configuration.HOSTNAME) + ":" + ctx.getNodeConfig().getIngestionServicePort();
    }

    /**
     * Method that converts a given key to an identifier of the range 0 - 2^128
     *
     * @param key String key
     * @return Identifier between 0 - 2^128
     */
    public static BigInteger getIdentifier(String key) {
        // hash the passed in key. Use it as an 2's complement of a number to construct
        // a positive BigInteger object.
        BigInteger identifier = null;
        try {
            identifier = new BigInteger(1, MessageDigest.getInstance("SHA-1").digest(key.getBytes()));
            identifier = identifier.mod(BASE);
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return identifier;
    }

    static String createZKDirectory(ZooKeeper zk, String path, CreateMode createMode) throws KeeperException, InterruptedException {
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
}
