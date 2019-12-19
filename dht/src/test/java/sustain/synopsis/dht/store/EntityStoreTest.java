package sustain.synopsis.dht.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;

public class EntityStoreTest {
    @TempDir
    File storageDir;

    @Mock
    DiskManager diskManagerMock;

    @Test
    void testSerializedFilePath() throws StorageException {
        EntityStore store = new EntityStore("9xj", "/tmp", 1024, 50);
        store.init(new CountDownLatch(1));
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        store.store(session, key1, value1);
        store.store(session, key2, value2);
        String returned = store.getSSTableOutputPath(session,"/tmp");
        String expected = "/tmp/9xj_" + key1.toString() + "_" + key2.toString() + "_0.sd";
        Assertions.assertEquals(expected, returned);
    }

    @Test
    void testToSSTable() throws StorageException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        EntityStore entityStore = new EntityStore("9xj", "/tmp", 1024, 50);
        entityStore.init(new CountDownLatch(1));
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.store(session, key1, value1);
        entityStore.store(session, key2, value2);
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        entityStore.toSSTable(session, diskManagerMock, metadata);

        File serializedSSTable = new File(storageDir.getAbsolutePath() + File.separator + "9xj" + "_" +
               key1.toString() + "_" + key2.toString() + "_" + 0 + ".sd");
        Assertions.assertTrue(serializedSSTable.exists());
        Assertions.assertTrue(serializedSSTable.isFile());
        Assertions.assertTrue(serializedSSTable.length() > 0);
    }
}
