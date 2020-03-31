package sustain.synopsis.dht;

import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * User provided configuration for a storage node. This bean class is a direct mapping between the YAML (v1.1)
 * configuration file provided during the start up. For each member variable, there should be corresponding
 * getter/setter methods.
 */
@SuppressWarnings("unused")
public class NodeConfiguration {

    public static final String HOSTNAME_PLACEHOLDER = "$HOSTNAME";
    private String hostname = Util.getHostname();
    private int ingestionServicePort;
    private List<String> zkEnsemble;
    private Map<String, Long> storageDirs;
    private String storageAllocationPolicy;
    private String rootJournalLoc;
    private int memTableSize;
    private int blockSize;
    private String metadataStoreDir;
    private int writerPoolSize;

    static NodeConfiguration fromYamlFile(String filePath) throws IOException {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            Yaml yaml = new Yaml(new Constructor(NodeConfiguration.class));
            return yaml.load(fis);
        }
    }

    public int getIngestionServicePort() {
        return ingestionServicePort;
    }

    public void setIngestionServicePort(int ingestionServicePort) {
        this.ingestionServicePort = ingestionServicePort;
    }

    public Map<String, Long> getStorageDirs() {
        return this.storageDirs;
    }

    public void setStorageDirs(Map<String, Long> storageDirs) {
        this.storageDirs = Collections.unmodifiableMap(storageDirs).entrySet().stream().collect(
                Collectors.toMap(x -> x.getKey().replace(HOSTNAME_PLACEHOLDER, hostname), Map.Entry::getValue));
    }

    public String getStorageAllocationPolicy() {
        return storageAllocationPolicy;
    }

    public void setStorageAllocationPolicy(String storageAllocationPolicy) {
        this.storageAllocationPolicy = storageAllocationPolicy;
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
        this.metadataStoreDir = metadataStoreDir.replace(HOSTNAME_PLACEHOLDER, hostname);
    }

    public int getWriterPoolSize() {
        return writerPoolSize;
    }

    public void setWriterPoolSize(int writerPoolSize) {
        this.writerPoolSize = writerPoolSize;
    }

    public List<String> getZkEnsemble() {
        return zkEnsemble;
    }

    public void setZkEnsemble(List<String> zkEnsemble) {
        this.zkEnsemble = zkEnsemble;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NodeConfiguration)) return false;
        NodeConfiguration that = (NodeConfiguration) o;
        return getIngestionServicePort() == that.getIngestionServicePort() && getMemTableSize() == that
                .getMemTableSize() && getBlockSize() == that.getBlockSize() && getWriterPoolSize() == that
                .getWriterPoolSize() && hostname.equals(that.hostname) && getStorageDirs().equals(that.getStorageDirs())
               && getStorageAllocationPolicy().equals(that.getStorageAllocationPolicy()) && getRootJournalLoc()
                       .equals(that.getRootJournalLoc()) && getMetadataStoreDir().equals(that.getMetadataStoreDir());
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, getIngestionServicePort(), getStorageDirs(), getStorageAllocationPolicy(),
                            getRootJournalLoc(), getMemTableSize(), getBlockSize(), getMetadataStoreDir(),
                            getWriterPoolSize());
    }
}
