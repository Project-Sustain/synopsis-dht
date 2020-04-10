package sustain.synopsis.dht;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import sustain.synopsis.dht.zk.ZKError;
import sustain.synopsis.dht.zk.ZooKeeperAgent;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * @author Thilina Buddhika
 */
public class Node {

    private final Logger logger = Logger.getLogger(Node.class);
    private final int port;
    private final BindableService[] services;
    private Server server;

    public Node(int port, BindableService[] services) {
        this.port = port;
        this.services = services;
    }

    public void start(boolean registerInZK) {
        try {
            logger.info("Trying to bind to port: " + this.port);
            // start gRPC services
            ServerBuilder<?> builder = ServerBuilder.forPort(port);
            Arrays.stream(services).forEach(builder::addService);
            this.server = builder.build().start();
            logger.info("Node is running on " + this.port);
            if (registerInZK) {
                // register in ZK
                try {
                    registerInZK();
                    // allow some time for the ZK write to sync in.
                    Thread.sleep(5 * 1000);
                } catch (ZKError zkError) {
                    logger.error("Error when registering with ZK. Shutting down.");
                    return;
                }
            }
            // start the membership changes listener thread.
            Ring ring = new Ring();
            new Thread(ring).start();
            Context ctx = Context.getInstance();
            ctx.setRing(ring);
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    logger.info("Shutting down the node.");
                    try {
                        Node.this.stop();
                    } catch (InterruptedException e) {
                        e.printStackTrace(System.err);
                    }
                    logger.info("Shutdown complete!");
                }
            });
            logger.info("Server startup is complete..");
            server.awaitTermination();
        } catch (InterruptedException e) {
            logger.error("Error starting the node. ", e);
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
                    String groupDir = Util.createZKDirectory(zk, ServerConstants.ZK_NODES_ROOT, CreateMode.PERSISTENT);
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

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }
}
