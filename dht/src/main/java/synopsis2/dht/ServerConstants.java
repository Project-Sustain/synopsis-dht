package synopsis2.dht;

/**
 * @author Thilina Buddhika
 */
public class ServerConstants {
    // Node configurations
    public class Configuration {
        public static final String PORT = "dht.port";
        public static final String ZK_SERVERS = "zk.servers";
        public static final String NODE_TYPE = "node.type";
        public static final String HOSTNAME = "hostname";
    }

    public class REPLACEABLE_STRINGS {
        public static final String HOSTNAME = "{HOSTNAME}";
    }
    
    public class NodeType {
        public static final String DATA = "spatial";
        public static final String METADATA = "temporal";
    }

    public static final String ZK_NODES_ROOT = "/gossamer_nodes";

    public class DATA_DISPERSION_SCHEME{
        public static final String CONSISTENT_HASHING = "consistent-hashing";
    }
}
