package sustain.synopsis.dht;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import sustain.synopsis.dht.services.IngestionService;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.zk.ZKError;
import sustain.synopsis.dht.zk.ZooKeeperAgent;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class Node {

    private final Logger logger = Logger.getLogger(Node.class);
    private final int port;
    private Server server;

    public Node(int port) {
        this.port = port;
    }

    public void start() {
        try {
            logger.info("Trying to bind to port: " + this.port);
            // start gRPC services
            this.server = ServerBuilder.forPort(port).addService(new IngestionService()).build().start();
            logger.info("Node is running on " + this.port);
            // register in ZK
            try {
                registerInZK();
            } catch (ZKError zkError) {
                logger.error("Error when registering with ZK. Shutting down.");
                return;
            }
            // allow some time for the ZK write to sync in.
            Thread.sleep(5 * 1000);
            // start the membership changes listener thread.
            Ring ring = new Ring();
            new Thread(ring).start();
            Context ctx = Context.getInstance();
            ctx.setRing(ring);
            logger.info("Shutting down the node.");
        } catch (InterruptedException e) {
            logger.error("Error starting the node. ", e);
        } catch (StorageException e) {
            logger.error("Error starting the NodeStore.", e);
        } catch (IOException e) {
           logger.error("Error starting the gRPC services.", e);
        } catch (ZKError zkError) {
            logger.error("Error initializing the ring with zk client.", zkError);
        }
    }

    private void registerInZK() throws ZKError {
        String nodeAddress = Util.getNodeAddress();
        ZooKeeper zk = ZooKeeperAgent.getInstance().getZooKeeperInstance();
        // create the group membership directory as well as stream directory
        try {
            try {
                if (zk.exists(ServerConstants.ZK_NODES_ROOT, false) == null) {
                    String groupDir = Util.createZKDirectory(zk, ServerConstants.ZK_NODES_ROOT,
                            CreateMode.PERSISTENT);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Created Root ZNode: " + groupDir);
                    }
                }
            } catch (KeeperException e) {
                // ignore. It is likely that the directory is already created by some other node.
            }
            int virtualNodeCount = Util.getVirtualNodeCount();
            for (int i = 0; i < virtualNodeCount; i++) {
                String individualDir = ServerConstants.ZK_NODES_ROOT + "/" + nodeAddress + ":" + i;
                try {
                    Util.createZKDirectory(zk, individualDir, CreateMode.EPHEMERAL);
                } catch (KeeperException e) {
                    logger.error(e.getMessage(), e);
                    throw new ZKError(e.getMessage(), e);
                }
            }

        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
            throw new ZKError(e.getMessage(), e);
        }
    }
}
