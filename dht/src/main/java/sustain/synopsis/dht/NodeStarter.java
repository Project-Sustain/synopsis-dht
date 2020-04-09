package sustain.synopsis.dht;

import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.services.ingestion.IngestionRequestDispatcher;
import sustain.synopsis.dht.services.ingestion.IngestionService;
import sustain.synopsis.dht.services.query.TargetedQueryService;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.node.NodeStore;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * @author Thilina Buddhika
 */
public class NodeStarter {

    public static Logger logger = Logger.getLogger(NodeStarter.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            logger.error("Path to the configuration file is missing. Exiting!");
            return;
        }
        String configFilePath = args[0];
        logger.info("Using the configuration: " + configFilePath);
        // initialize context
        Context ctx = Context.getInstance();
        try {
            ctx.initialize(configFilePath);
        } catch (IOException e) {
            logger.error("Error initializing node config. Config file not found.", e);
            return;
        }
        // set the hostname
        ctx.setProperty(ServerConstants.HOSTNAME, Util.getHostname());
        logger.info("Successfully initialized node context.");

        int port = ctx.getNodeConfig().getIngestionServicePort();
        try {
            NodeStore nodeStore = new NodeStore();
            nodeStore.init();
            BindableService[] services =
                    new BindableService[]{new IngestionService(new IngestionRequestDispatcher(nodeStore)),
                            new TargetedQueryService(nodeStore)};
            Node node = new Node(port, services);
            // this is a blocking call
            CountDownLatch latch = new CountDownLatch(1);
            node.start(latch);
            latch.await();
            logger.info("Server start up is complete!");
        } catch (StorageException e) {
            logger.error("Error initiating the ingestion service.", e);
        } catch (InterruptedException e) {
            logger.error("Interrupted while waiting for the server to start.", e);
        }
    }
}
