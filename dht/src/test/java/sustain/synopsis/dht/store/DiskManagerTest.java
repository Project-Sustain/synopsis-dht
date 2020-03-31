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
import java.util.ArrayList;
import java.util.List;

public class DiskManagerTest {

    @Mock
    File mockFile;

    @Mock
    File mockFile2;

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
        fw.append("storageAllocationPolicy: 'round-robin'\n");
        fw.flush();
        fw.close();
        Context.getInstance().initialize(nodeConfigFilePath.toAbsolutePath().toString());
    }

    @Test
    void testProcessPath() throws StorageException, IOException {
        // existing dir
        Mockito.when(mockFile.exists()).thenReturn(true);
        DiskManager diskManager = new DiskManager();
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
        Assertions.assertEquals("/a/b/c", directory.path.getAbsolutePath());
        Assertions.assertEquals(100L, directory.availableSpace);
        Assertions.assertEquals(1024, directory.allocatedCapacity);
    }

    @Test
    void testDiskManagerInit() throws IOException {
        initContext();
        DiskManager diskManager = new DiskManager();
        Assertions.assertFalse(diskManager.init(null));
        Assertions.assertFalse(diskManager.init(new NodeConfiguration()));
        Assertions.assertTrue(diskManager.init(Context.getInstance().getNodeConfig()));
    }

    @Test
    void testAllocate() {
        // successful allocation
        DiskManager.StorageDirectory dir = new DiskManager.StorageDirectory(new File("/tmp/"), 1024, 0, 2048);
        Assertions.assertTrue(dir.allocate(512));
        Assertions.assertTrue(dir.allocate(512));
        Assertions.assertFalse(dir.allocate(100));

        // check the buffer
        dir = new DiskManager.StorageDirectory(new File("/tmp/"), 1024, 0, 2048);
        Assertions.assertFalse(dir.allocate(1127));
        Assertions.assertTrue(dir.allocate(1100));

        // available space is low
        Mockito.when(mockFile.getUsableSpace()).thenReturn(512L);
        dir = new DiskManager.StorageDirectory(mockFile, 1024, 0, 512);
        Assertions.assertFalse(dir.allocate(768));

        // check if the available space is updated during an allocation
        dir = new DiskManager.StorageDirectory(mockFile, 1024, 0, 2048);
        dir.allocate(512);
        Assertions.assertTrue(dir.availableSpace < 2048);
    }

    @Test
    void testRoundRobinAllocationPolicy() {
        RoundRobinAllocationPolicy policy = new RoundRobinAllocationPolicy();
        List<DiskManager.StorageDirectory> dirs = new ArrayList<>(3);
        dirs.add(0, new DiskManager.StorageDirectory(mockFile, 1024, 0, 2048));
        dirs.add(1, new DiskManager.StorageDirectory(mockFile2, 1024, 0, 2048));

        Mockito.when(mockFile.getUsableSpace()).thenReturn(2048L);
        Mockito.when(mockFile2.getUsableSpace()).thenReturn(2048L);
        Assertions.assertEquals(dirs.get(0), policy.select(128, dirs));
        Assertions.assertEquals(dirs.get(1), policy.select(128, dirs));
        Assertions.assertEquals(dirs.get(0), policy.select(128, dirs));

        // first dir does not have enough space
        Mockito.when(mockFile.getUsableSpace()).thenReturn(10L);
        policy = new RoundRobinAllocationPolicy();
        Assertions.assertEquals(dirs.get(1), policy.select(128, dirs));

        // both dirs do not have enough space
        Mockito.when(mockFile2.getUsableSpace()).thenReturn(10L);
        Assertions.assertNull(policy.select(128, dirs));
    }

    @Test
    void testAllocationPolicyFactory() {
        Assertions.assertEquals(RoundRobinAllocationPolicy.class,
                                AllocationPolicyFactory.getAllocationPolicy("round" + "-robin").getClass());
        Assertions.assertNull(AllocationPolicyFactory.getAllocationPolicy("unknown"));
    }

}
