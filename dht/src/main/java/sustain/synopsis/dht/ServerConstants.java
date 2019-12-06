package sustain.synopsis.dht;

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
        public static final String KAFKA_BOOTSTRAP_BROKERS = "kafka.bootstrap.brokers";
        public static final String STRAND_INGESTION_CONSUMER_GROUP_ID = "strand.ingestion.consumer.group.id";
        public static final String STRAND_INGESTION_TOPIC_PREFIX = "strand.ingestion.topic.prefix";
    }

    public class TIMEOUTS{
        public static final int RELAY_WRITE_TIMEOUT_S = 3;
        public static final int RELAY_READ_TIMEOUT_S = 3;
        public static final int KAFKA_CONSUMER_HEARTBEAT_INTERVAL_MS = 4;
        public static final int KAFKA_CONSUMER_SESSION_TIMEOUT = 20;
    }

    public class REPLACEABLE_STRINGS {
        public static final String HOSTNAME = "{HOSTNAME}";
    }
    
    public class NodeType {
        public static final String DATA = "spatial";
        public static final String TEMPORAL = "temporal";
    }

    public static final String ZK_NODES_ROOT = "/gossamer_nodes";

    public class DATA_DISPERSION_SCHEME{
        public static final String CONSISTENT_HASHING = "consistent-hashing";
    }
}
