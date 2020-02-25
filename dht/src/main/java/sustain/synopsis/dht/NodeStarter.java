package sustain.synopsis.dht;

import org.apache.log4j.Logger;

import java.io.IOException;

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
        ctx.setProperty(ServerConstants.Configuration.HOSTNAME, Util.getHostname());
        logger.info("Successfully initialized node context.");

        int port = Integer.parseInt(ctx.getProperty(ServerConstants.Configuration.PORT));
        Node node = new Node(port);
        // this is a blocking call
        node.start();
        Runtime.getRuntime().exit(-1);
    }
}
