package sustain.synopsis.dht.zk;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import sustain.synopsis.dht.Context;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperAgent implements Watcher {

    private static ZooKeeperAgent instance;
    private final Logger logger = Logger.getLogger(ZooKeeperAgent.class);
    private final ZooKeeper zk;
    private CountDownLatch connectionWatcher = new CountDownLatch(1);

    private ZooKeeperAgent() throws ZKError {
        try {
            List<String> zkEnsemble = Context.getInstance().getNodeConfig().getZkEnsemble();
            this.zk = new ZooKeeper(String.join(",", zkEnsemble), 30000, this);
            logger.info("Waiting for ZooKeeper connection.");
            this.connectionWatcher.await();
            logger.info("Successfully connected with Zookeeper cluster.");
        } catch (InterruptedException | IOException ex) {
            logger.error(ex.getMessage(), ex);
            throw new ZKError(ex.getMessage(), ex);
        }
    }

    public static ZooKeeperAgent getInstance() throws ZKError {
        if (instance == null) {
            synchronized (ZooKeeperAgent.class) {
                if (instance == null) {
                    instance = new ZooKeeperAgent();
                }
            }
        }
        return instance;
    }

    public void process(WatchedEvent watchedEvent) {
        if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
            this.connectionWatcher.countDown();
        }

    }

    public ZooKeeper getZooKeeperInstance() {
        return this.zk;
    }
}

