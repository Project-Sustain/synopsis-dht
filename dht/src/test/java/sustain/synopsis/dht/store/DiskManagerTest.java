package sustain.synopsis.dht.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

public class DiskManagerTest {

    @Mock
    File mockFile;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    void initContext() throws IOException {
        Path nodeConfigFilePath = tempDir.resolve("test.yaml");
        FileWriter fw = new FileWriter(nodeConfigFilePath.toFile());
        fw.append("---\n");
        fw.append("storageDirs:\n");
        fw.append("  /tmp/ : 1024\n");
        fw.flush();
        fw.close();
        Context.getInstance().initialize(new Properties(), nodeConfigFilePath.toAbsolutePath().toString());
    }

    @Test
    void testProcessPath() throws StorageException, IOException {
        initContext();
        // existing dir
        Mockito.when(mockFile.exists()).thenReturn(true);
        DiskManager diskManager = DiskManager.getInstance();
        diskManager.processPath(mockFile, 1024);
        Mockito.verify(mockFile, Mockito.times(0)).mkdirs();

        // non existing dir
        Mockito.when(mockFile.exists()).thenReturn(false);
        diskManager.processPath(mockFile, 1024);
        Mockito.verify(mockFile, Mockito.times(1)).mkdirs();

        // error creating the dir
        Mockito.when(mockFile.exists()).thenReturn(false);
        Mockito.when(mockFile.mkdirs()).thenReturn(false);
        Assertions.assertNull(diskManager.processPath(mockFile, 1024));

        // path points to a file
        Mockito.when(mockFile.isDirectory()).thenReturn(false);
        Assertions.assertNull(diskManager.processPath(mockFile, 1024));

        // no available space in the partition
        Mockito.when(mockFile.getFreeSpace()).thenReturn(0L);
        Assertions.assertNull(diskManager.processPath(mockFile, 1024));

        // success case
        Mockito.when(mockFile.exists()).thenReturn(true);
        Mockito.when(mockFile.isDirectory()).thenReturn(true);
        Mockito.when(mockFile.getFreeSpace()).thenReturn(100L);
        Mockito.when(mockFile.getAbsolutePath()).thenReturn("/a/b/c");
        DiskManager.StorageDirectory directory = diskManager.processPath(mockFile, 1024);
        Assertions.assertNotNull(directory);
        Assertions.assertEquals("/a/b/c", directory.path);
        Assertions.assertEquals(100L, directory.availableSpace);
        Assertions.assertEquals(1024, directory.allocatedCapacity);
    }

    @Test
    void testDiskManagerInit() throws StorageException, IOException {
        initContext();
        DiskManager diskManager = DiskManager.getInstance();
        Assertions.assertFalse(diskManager.init(null));
        Assertions.assertFalse(diskManager.init(new NodeConfiguration()));

        Assertions.assertTrue(diskManager.init(Context.getInstance().getNodeConfig()));
    }

    @Test
    void testSingletonInstanceWithFailedInit() throws StorageException {
        Context.getInstance().initialize(new Properties());
        Assertions.assertThrows(StorageException.class, () -> {
            DiskManager.getInstance();
        });
    }

    @Test
    void testSingletonInstanceWithSuccessfulInit() throws StorageException, IOException {
        initContext();
        Assertions.assertNotNull(DiskManager.getInstance());
    }

}
