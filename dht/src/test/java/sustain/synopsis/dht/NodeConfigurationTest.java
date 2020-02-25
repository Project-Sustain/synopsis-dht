package sustain.synopsis.dht;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

public class NodeConfigurationTest {

    @TempDir
    Path tempDir;

    @Test
    void testGetters() throws IOException {
        Path nodeConfigFilePath = tempDir.resolve("test.yaml");
        serializeNodeConfig(nodeConfigFilePath.toFile());
        NodeConfiguration configuration = NodeConfiguration.fromYamlFile(nodeConfigFilePath.toAbsolutePath().toString());

        Assertions.assertEquals(9091, configuration.getIngestionServicePort());
        Map<String, Long> storageDirs = configuration.getStorageDirs();
        Assertions.assertEquals(1, storageDirs.size());
        Assertions.assertEquals(1024L, storageDirs.get("/tmp/"));
        Assertions.assertEquals("round-robin", configuration.getStorageAllocationPolicy());
        Assertions.assertEquals("/tmp/root-journal.slog", configuration.getRootJournalLoc());
        Assertions.assertEquals(50, configuration.getMemTableSize());
        Assertions.assertEquals(10, configuration.getBlockSize());
        Assertions.assertEquals(5, configuration.getWriterPoolSize());
    }

    @Test
    void testHostnameOverriding() throws IOException {
        Path nodeConfigFilePath = tempDir.resolve("test.yaml");
        FileWriter fw = new FileWriter(nodeConfigFilePath.toFile());
        fw.append("---\n");
        fw.append("storageDirs:\n");
        fw.append("  /" + NodeConfiguration.HOSTNAME_PLACEHOLDER + "/tmp/ : 1024\n");
        fw.append("storageAllocationPolicy: 'round-robin'\n");
        fw.append("rootJournalLoc: '/" + NodeConfiguration.HOSTNAME_PLACEHOLDER + "/tmp/root-journal.slog'\n");
        fw.flush();
        fw.close();
        NodeConfiguration configuration = NodeConfiguration.fromYamlFile(nodeConfigFilePath.toAbsolutePath().toString());

        String hostname = Util.getHostname();
        Assertions.assertEquals("/" + hostname + "/tmp/",
                configuration.getStorageDirs().keySet().iterator().next());
        Assertions.assertEquals("/" + hostname + "/tmp/root-journal.slog", configuration.getRootJournalLoc());
    }

    public static void serializeNodeConfig(File output) throws IOException {
        FileWriter fw = new FileWriter(output);
        fw.append("---\n");
        fw.append("ingestionServicePort: 9091\n");
        fw.append("storageDirs:\n");
        fw.append("  /tmp/ : 1024\n");
        fw.append("storageAllocationPolicy: 'round-robin'\n");
        fw.append("rootJournalLoc: '/tmp/root-journal.slog'\n");
        fw.append("memTableSize: 50\n");
        fw.append("blockSize: 10\n");
        fw.append("metadataStoreDir: '/tmp'\n");
        fw.append("writerPoolSize: 5\n");
        fw.flush();
        fw.close();
    }
}
