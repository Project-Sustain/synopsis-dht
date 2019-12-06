package sustain.synopsis.dht;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

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
        // initialize the context
        Properties properties;
        try {
            properties = Util.loadAsProperties(configFilePath);
        } catch (IOException e) {
            return;
        }
        // initialize context
        Context ctx = Context.getInstance();
        ctx.initialize(properties);
        // set the hostname
        ctx.addProperty(ServerConstants.Configuration.HOSTNAME, Util.getHostname());

        logger.info("Successfully initialized Gossamer Context.");

        int port = Integer.parseInt(ctx.getProperty(ServerConstants.Configuration.PORT));
        Node node = new Node(port);
        // this is a blocking call
        node.start();
        Runtime.getRuntime().exit(-1);
    }
}
