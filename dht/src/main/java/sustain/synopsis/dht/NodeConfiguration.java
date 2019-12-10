package sustain.synopsis.dht;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;

/**
 * User provided configuration for a storage node. This bean class is a direct mapping between
 * the YAML (v1.1) configuration file provided during the start up.
 * For each member variable, there should be corresponding getter/setter methods.
 */
@SuppressWarnings("unused")
public class NodeConfiguration {

    static NodeConfiguration fromYamlFile(String filePath) throws FileNotFoundException {
        FileInputStream fis;
        fis = new FileInputStream(filePath);
        Yaml yaml = new Yaml(new Constructor(NodeConfiguration.class));
        return yaml.load(fis);
    }

    private Map<String, Long> storageDirs;

    public Map<String, Long> getStorageDirs() {
        if(this.storageDirs != null) {
            return Collections.unmodifiableMap(this.storageDirs);
        } else {
            return null;
        }
    }

    public void setStorageDirs(Map<String, Long> storageDirs) {
        if (this.storageDirs == null) {
            this.storageDirs = storageDirs;
        }
    }
}
