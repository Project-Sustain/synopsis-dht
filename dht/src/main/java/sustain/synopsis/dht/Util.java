package sustain.synopsis.dht;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Properties;

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
        return ctx.getProperty(ServerConstants.Configuration.HOSTNAME) + ":" +
                ctx.getProperty(ServerConstants.Configuration.PORT);
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

    static String createZKDirectory(ZooKeeper zk, String path, byte[] data, CreateMode createMode) throws
            KeeperException, InterruptedException {
        try {
            return zk.create(path, data,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    createMode);
        } catch (InterruptedException | KeeperException e) {
            throw e;
        }
    }

    public static int getVirtualNodeCount() {
        double headSizeInGB = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / (1024.0 * 1024 * 1024);
        int vNodeCount = 200; // 200 works well with NOAA data.
        if (headSizeInGB != -1) {
            if (headSizeInGB >= 11) {
                vNodeCount = 200;
            } else if (headSizeInGB >= 7) {
                vNodeCount = 150;
            } else {
                vNodeCount = 100;
            }
        }
        logger.info("Maximum heap size: " + headSizeInGB + ", allocated virtual node count: " + vNodeCount);
        return vNodeCount;
    }

    public static Properties loadAsProperties(String path) throws IOException {
        Properties properties = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(path);
            properties.load(fis);
        } catch (FileNotFoundException e) {
            logger.error("Incorrect path to the config file. ", e);
            throw e;
        } catch (IOException e) {
            logger.error("Error parsing the config file.", e);
            throw e;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException e) {
                    logger.error("Error closing the stream.");
                }
            }
        }
        return properties;
    }

    public static int getWorkerPoolSize() {
        return Runtime.getRuntime().availableProcessors() * 2;
    }

    public static long localDateTimeToEpoch(LocalDateTime localDateTime){
        ZonedDateTime zdt = localDateTime.atZone(ZoneId.of("UTC"));
        return zdt.toInstant().toEpochMilli();
    }

    public static LocalDateTime epochToLocalDateTime(long startTS) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(startTS), ZoneId.of("UTC"));
    }
}
