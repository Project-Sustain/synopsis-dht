package sustain.synopsis.dht;

import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.services.ingestion.IngestionRequestProcessor;
import sustain.synopsis.dht.services.ingestion.IngestionService;
import sustain.synopsis.dht.services.query.TargetedQueryService;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.node.NodeStore;

import java.io.IOException;

/**
 * @author Thilina Buddhika
 */
public class DHTNodeStarter {

    public static Logger logger = Logger.getLogger(DHTNodeStarter.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            logger.error("Path to the configuration file is missing. Exiting!");
            return;
        }
        String configFilePath = args[0];
        logger.info("Using the configuration: " + configFilePath);

        Context ctx = Context.getInstance();
        try {
            ctx.initialize(configFilePath);  // set the hostname
            ctx.setProperty(ServerConstants.HOSTNAME, Util.getHostname());
            logger.info("Successfully initialized node context.");
        } catch (IOException e) {
            logger.error("Error initializing node config. Config file not found.", e);
            return;
        }

        try {
            NodeStore nodeStore = new NodeStore();
            nodeStore.init();
            Node node = new Node(ctx.getNodeConfig().getIngestionServicePort(),
                                 new BindableService[]{new IngestionService(new IngestionRequestProcessor(nodeStore)),
                                         new TargetedQueryService(nodeStore)});
            node.start(true);
        } catch (StorageException e) {
            logger.error("Error initializing the NodeStore.", e);
            }
    }
}
