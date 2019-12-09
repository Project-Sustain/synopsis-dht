package sustain.synopsis.dht;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;

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

    private List<String> storageDirs;

    public List<String> getStorageDirs() {
        return storageDirs;
    }

    public void setStorageDirs(List<String> storageDirs) {
        this.storageDirs = storageDirs;
    }
}
