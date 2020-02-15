package sustain.synopsis.dht;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Thilina Buddhika
 */
public class Context {

    private static Context instance = new Context();
    private ConcurrentHashMap<String, String> properties = new ConcurrentHashMap<>();
    private Ring ring;
    private NodeConfiguration nodeConfig;

    private Context() {
    }

    public static Context getInstance() {
        return instance;
    }

    public void initialize(Properties properties) {
        for (String prop : properties.stringPropertyNames()) {
            this.properties.put(prop, localize(properties.getProperty(prop)));
        }
    }

    public void initialize(Properties properties, String nodeConfigPath) throws FileNotFoundException {
        initialize(properties);
        nodeConfig = NodeConfiguration.fromYamlFile(nodeConfigPath);
    }

    // used mainly for unit testing by injecting a custom nodeConfiguration
    public void initialize(Properties properties, NodeConfiguration nodeConfiguration){
        initialize(properties);
        this.nodeConfig = nodeConfiguration;
    }

    private String localize(String propertyName) {
        if (propertyName.contains(ServerConstants.REPLACEABLE_STRINGS.HOSTNAME)) {
            return propertyName.replace(ServerConstants.REPLACEABLE_STRINGS.HOSTNAME, Util.getHostname());
        }
        return propertyName;
    }

    public void addProperty(String name, String val) {
        this.properties.put(name, val);
    }

    public String getProperty(String propName) {
        return properties.get(propName);
    }

    public Map<String, String> getPropertiesWithPrefix(String prefix) {
        Map<String, String> matchedProperties = new HashMap<>();
        for (String property : this.properties.keySet()) {
            if (property.toLowerCase().startsWith(prefix.toLowerCase())) {
                matchedProperties.put(property, this.properties.get(property));
            }
        }
        return matchedProperties;
    }

    public Ring getRing() {
        return ring;
    }

    public void setRing(Ring ring) {
        this.ring = ring;
    }

    public NodeConfiguration getNodeConfig() {
        return nodeConfig;
    }
}
