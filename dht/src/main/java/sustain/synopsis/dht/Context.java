package sustain.synopsis.dht;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Context is a singleton object available to the DHT node runtime to access node-wide configuration. This includes the
 * startup configurations, runtime properties and objects.
 */
public class Context {

    private static Context instance = new Context();
    private final Map<String, String> properties = new HashMap<>();
    private Ring ring;
    private NodeConfiguration nodeConfig;
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private Context() {
    }

    public static Context getInstance() {
        return instance;
    }

    public void initialize(String nodeConfigPath) throws IOException {
        try {
            lock.writeLock().lock();
            nodeConfig = NodeConfiguration.fromYamlFile(nodeConfigPath);
        } finally {
            lock.writeLock().unlock();
        }
    }

    // used mainly for unit testing by injecting a custom nodeConfiguration
    public void initialize(NodeConfiguration nodeConfiguration) {
        try {
            lock.writeLock().lock();
            this.nodeConfig = nodeConfiguration;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public void setProperty(String name, String val) {
        try {
            lock.writeLock().lock();
            this.properties.put(name, val);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public String getProperty(String propName) {
        try {
            lock.readLock().lock();
            return properties.get(propName);
        } finally {
            lock.readLock().unlock();
        }
    }

    public Ring getRing() {
        try {
            lock.readLock().lock();
            return ring;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void setRing(Ring ring) {
        try {
            lock.writeLock().lock();
            this.ring = ring;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public NodeConfiguration getNodeConfig() {
        try {
            lock.readLock().lock();
            if (nodeConfig == null) {
                throw new RuntimeException("Accessing uninitialized context.");
            }
            return nodeConfig;
        } finally {
            lock.readLock().unlock();
        }
    }
}
