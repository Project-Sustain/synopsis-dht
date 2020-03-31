package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;

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

    static String createZKDirectory(ZooKeeper zk, String path, CreateMode createMode)
            throws KeeperException, InterruptedException {
        return zk.create(path, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, createMode);
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
