package sustain.synopsis.dht.store.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;

public class EntityStoreTest {
    @TempDir
    File storageDir;

    @TempDir
    File metadataDir;

    @Mock
    DiskManager diskManagerMock;

    @Mock
    EntityStoreJournal entityStoreJournalMock;

    @Test
    void testSerializedFilePath() throws StorageException, IOException {
        EntityStore store = new EntityStore("9xj", metadataDir.getAbsolutePath(), 1024, 50);
        store.init();
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        String returned = store.getSSTableOutputPath(key1, key2, storageDir.getAbsolutePath(), 0);
        String expected =
                storageDir.getAbsolutePath() + File.separator + "9xj_" + key1.toString() + "_" + key2.toString() +
                        "_0.sd";
        Assertions.assertEquals(expected, returned);
    }

    @Test
    void testToSSTable() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        EntityStore entityStore = new EntityStore("9xj", metadataDir.getAbsolutePath(), 1024, 50);
        entityStore.init(diskManagerMock);
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        entityStore.store(session, key1, value1);
        entityStore.store(session, key2, value2);
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        entityStore.toSSTable(session, diskManagerMock, metadata);

        File serializedSSTable =
                new File(storageDir.getAbsolutePath() + File.separator + "9xj" + "_" + key1.toString() + "_" + key2.toString() + "_0.sd");
        Assertions.assertTrue(serializedSSTable.exists());
        Assertions.assertTrue(serializedSSTable.isFile());
        Assertions.assertTrue(serializedSSTable.length() > 0);
        Assertions.assertEquals(serializedSSTable.getAbsolutePath(), metadata.getPath());
    }

    @Test
    void testStoreStrand() throws StorageException, IOException {     // testing the entire flow
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        Mockito.when(entityStoreJournalMock.init()).thenReturn(true);
        Mockito.when(entityStoreJournalMock.getSequenceId()).thenReturn(0);
        Mockito.when(entityStoreJournalMock.getMetadata()).thenReturn(new ArrayList<>());
        EntityStore entityStore = new EntityStore("noaa:9xj", entityStoreJournalMock, 200, 50);
        entityStore.init(diskManagerMock);
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).init();

        // start a new session
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);

        Assertions.assertEquals(1, entityStore.activeSessions.size());
        Assertions.assertEquals(1, entityStore.activeMetadata.size());

        // size of key and value used here - 187 bytes
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        entityStore.store(session, key1, value1); // this should fill up the memTable
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        entityStore.store(session, key2, value2);
        // storing 2 strands should fill out the memTable
        Mockito.verify(diskManagerMock, Mockito.times(1)).allocate(Mockito.anyLong());
        Assertions.assertEquals(0, entityStore.queryiableMetadata.size());
        Metadata<StrandStorageKey> metadata = entityStore.activeMetadata.get(session).get(0);
        Assertions.assertEquals(key1, metadata.getMin());
        Assertions.assertEquals(key2, metadata.getMax());
        Assertions.assertEquals(entityStore.getSSTableOutputPath(key1, key2, storageDir.getAbsolutePath(), 0),
                metadata.getPath());

        StrandStorageKey key3 = new StrandStorageKey(1391216400200L, 1391216400300L);
        StrandStorageValue value3 = new StrandStorageValue(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0,
                2.0));
        entityStore.store(session, key3, value3);
    }

}
