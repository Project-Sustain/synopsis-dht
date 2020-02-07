package sustain.synopsis.dht;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * User provided configuration for a storage node. This bean class is a direct mapping between
 * the YAML (v1.1) configuration file provided during the start up.
 * For each member variable, there should be corresponding getter/setter methods.
 */
@SuppressWarnings("unused")
public class NodeConfiguration {

    public static final String HOSTNAME_PLACEHOLDER = "$HOSTNAME";

    static NodeConfiguration fromYamlFile(String filePath) throws FileNotFoundException {
        FileInputStream fis;
        fis = new FileInputStream(filePath);
        Yaml yaml = new Yaml(new Constructor(NodeConfiguration.class));
        return yaml.load(fis);
    }


    private String hostname = Util.getHostname();
    private Map<String, Long> storageDirs;
    private String storageAllocationPolicy;
    private String rootJournalLoc;
    private int memTableSize;
    private int blockSize;
    private String metadataStoreDir;

    public Map<String, Long> getStorageDirs() {
        if(this.storageDirs != null) {
            return this.storageDirs;
        } else {
            return null;
        }
    }

    public void setStorageDirs(Map<String, Long> storageDirs) {
        if (this.storageDirs == null) {
            this.storageDirs = Collections.unmodifiableMap(storageDirs).entrySet().stream().collect(
                    Collectors.toMap(x -> x.getKey().replace(HOSTNAME_PLACEHOLDER, hostname), Map.Entry::getValue));;
        }
    }

    public String getStorageAllocationPolicy() {
        return storageAllocationPolicy;
    }

    public void setStorageAllocationPolicy(String storageAllocationPolicy) {
        if (this.storageAllocationPolicy == null) {
            this.storageAllocationPolicy = storageAllocationPolicy;
        }
    }

    public String getRootJournalLoc() {
        return rootJournalLoc;
    }

    public void setRootJournalLoc(String rootJournalLoc) {
        this.rootJournalLoc = rootJournalLoc.replace(HOSTNAME_PLACEHOLDER, hostname);
    }

    public int getMemTableSize() {
        return memTableSize;
    }

    public void setMemTableSize(int memTableSize) {
        this.memTableSize = memTableSize;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }

    public String getMetadataStoreDir() {
        return metadataStoreDir;
    }

    public void setMetadataStoreDir(String metadataStoreDir) {
        this.metadataStoreDir = metadataStoreDir;
    }
}
