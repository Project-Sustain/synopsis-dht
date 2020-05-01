package sustain.synopsis.benchmarks.dht;

import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.*;
import sustain.synopsis.dht.zk.ZKError;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class BenchmarkBaseNode {
    private final Logger logger = Logger.getLogger(BenchmarkBaseNode.class);
    private final String configFilePath;
    private boolean initialized = false;

    public BenchmarkBaseNode(String configFile) {
        this.configFilePath = configFile;
    }

    protected boolean initContext() {
        if (initialized) {
            return false;
        }
        logger.info("Using the configuration: " + configFilePath);
        // initialize context
        Context ctx = Context.getInstance();
        try {
            ctx.initialize(configFilePath);
            Ring ring = new Ring();
            new Thread(ring).start(); // start the membership changes listener thread.
            ctx.setRing(ring);
        } catch (IOException | ZKError e) {
            logger.error("Error initializing node config. Config file not found.", e);
            return false;
        }
        // set the hostname
        ctx.setProperty(ServerConstants.HOSTNAME, Util.getHostname());
        this.initialized = true;
        logger.info("Successfully initialized node context.");
        return true;
    }

    protected void start(BindableService... services) throws InterruptedException {
        if (!initialized) {
            boolean success = initContext();
            if (!success) {
                throw new RuntimeException("Initialization failed.");
            }
        }
        CountDownLatch latch = new CountDownLatch(1);
        Thread serverT = new Thread(() -> {
            int port = Context.getInstance().getNodeConfig().getIngestionServicePort();
            Node node = new Node(port, services);
            node.start(true, latch);
        });
        serverT.start();
        latch.await();
    }
}
