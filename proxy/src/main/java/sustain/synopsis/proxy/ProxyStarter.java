package sustain.synopsis.proxy;

import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.*;
import sustain.synopsis.dht.services.ingestion.IngestionService;
import sustain.synopsis.dht.services.query.TargetedQueryService;
import sustain.synopsis.dht.zk.ZKError;
import sustain.synopsis.proxy.ingestion.ProxyIngestionRequestProcessor;
import sustain.synopsis.proxy.query.ProxyQueryProcessor;

import java.io.IOException;

public class ProxyStarter {

    public static Logger logger = Logger.getLogger(ProxyStarter.class);

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
            // start the membership changes listener thread.
            Ring ring = new Ring();
            new Thread(ring).start();
            ctx.setRing(ring);
        } catch (IOException | ZKError e) {
            logger.error("Error initializing the context.", e);
            return;
        }
        // set the hostname
        ctx.setProperty(ServerConstants.HOSTNAME, Util.getHostname());
        logger.info("Successfully initialized node context.");

        Node node = new Node(ctx.getNodeConfig().getIngestionServicePort(),
                             new BindableService[]{new IngestionService(new ProxyIngestionRequestProcessor()),
                                     new TargetedQueryService(new ProxyQueryProcessor())});
        node.start(false); // proxy servers should not register in ZK
    }
}

