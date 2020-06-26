package sustain.synopsis.metadata;

import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.*;
import sustain.synopsis.dht.zk.ZKError;


import java.io.IOException;

public class MetadataStarter {

    public static Logger logger = Logger.getLogger(MetadataStarter.class);

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            logger.error("Path to the configuration file is missing. Exiting!");
            return;
        }
        String configFilePath = args[0];
        logger.info("Using the configuration: " + configFilePath);

        NodeConfiguration nc = NodeConfiguration.fromYamlFile(configFilePath);

        Node node = new Node(
                nc.getMetadataServicePort(),
                new BindableService[]{
                        new MetadataService(new MetadataServiceRequestProcessor(nc.getMetadataJournalLoc()))
                }
            );
        node.start(false); // proxy servers should not register in ZK
    }

}

