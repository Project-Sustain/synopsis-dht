package sustain.synopsis.dht.store.node;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.DiskManager;
import sustain.synopsis.dht.store.IngestionSession;
import sustain.synopsis.dht.store.SessionValidator;
import sustain.synopsis.dht.store.StorageException;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;

public class NodeStoreTest {

    @TempDir
    File metadataStoreDir;

    @TempDir
    File storageDir;

    @Mock
    Logger loggerMock;

    @Mock
    SessionValidator sessionValidatorMock;

    @Mock
    DiskManager diskManagerMock;

    @Test
    void testFreshStart() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(loggerMock.iterator()).thenReturn(new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public byte[] next() {
                return null;
            }
        });
        Mockito.when(sessionValidatorMock.validate("bob", "dataset_1", 1000L)).thenReturn(true);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());

        NodeStore nodeStore = new NodeStore(sessionValidatorMock, loggerMock, 1024, 200,
                metadataStoreDir.getAbsolutePath(), diskManagerMock);
        nodeStore.init();
        assertTrue(nodeStore.entityStoreMap.isEmpty());
        assertTrue(nodeStore.validatedSessionIds.isEmpty());
        nodeStore.store("bob", "dataset_1", "entity_1", 1000L, 123456L, createStrand("9xj", 1391216400000L,
                1391216400100L, 1.0, 2.0));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertTrue(nodeStore.entityStoreMap.containsKey("dataset_1"));
        assertTrue(nodeStore.entityStoreMap.get("dataset_1").containsKey("entity_1"));
        assertTrue(nodeStore.validatedSessionIds.contains(1000L));

        String entityCommitLogPath = metadataStoreDir.getAbsolutePath() + File.separator + "entity_1_metadata.slog";
        Mockito.verify(sessionValidatorMock, Mockito.times(1)).validate("bob", "dataset_1", 1000L);
        Mockito.verify(loggerMock, Mockito.times(1)).append(new CreateEntityStoreActivity("dataset_1", "entity_1",
                entityCommitLogPath).serialize());
        // we can verify if there is a log written into the entity store log to make sure the store request is made to
        // to the entity store
        // log appenders are initialized
        File entityCommitLog = new File(entityCommitLogPath);
        assertTrue(entityCommitLog.exists());

        // add more data for the same entity store
        nodeStore.store("bob", "dataset_1", "entity_1", 1000L, 123456L, createStrand("9xj", 1391216400100L,
                1391216400200L, 1.0, 2.0));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertEquals(1, nodeStore.entityStoreMap.get("dataset_1").size());

        // add data to a different entity store
        nodeStore.store("bob", "dataset_1", "entity_2", 1000L, 123456L, createStrand("9xj", 1391216400100L,
                1391216400200L, 1.0, 2.0));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertEquals(2, nodeStore.entityStoreMap.get("dataset_1").size());
        assertTrue(nodeStore.entityStoreMap.get("dataset_1").containsKey("entity_2"));

        // add data to a different dataset
        nodeStore.store("bob", "dataset_2", "entity_2", 1000L, 123456L, createStrand("9xj", 1391216400100L,
                1391216400200L, 1.0, 2.0));
        assertEquals(2, nodeStore.entityStoreMap.size());
        assertTrue(nodeStore.entityStoreMap.containsKey("dataset_2"));

        // test end_session
        nodeStore.endSession("dataset_2",  "bob", 1000L, 123456L);
    }

    @Test
    void testRestart() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(sessionValidatorMock.validate("bob", "dataset_1", 1000L)).thenReturn(true);
        Mockito.when(sessionValidatorMock.validate("bob", "dataset_2", 1000L)).thenReturn(true);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        nodeConfiguration.setMetadataStoreDir(metadataStoreDir.getAbsolutePath());
        nodeConfiguration.setMemTableSize(1024);
        nodeConfiguration.setBlockSize(256);
        nodeConfiguration.setRootJournalLoc(metadataStoreDir.getAbsolutePath());
        Map<String, Long> storageDirs = new HashMap<>();
        storageDirs.put(storageDir.getAbsolutePath(), 10240L);
        nodeConfiguration.setStorageDirs(storageDirs);
        nodeConfiguration.setStorageAllocationPolicy("round-robin");
        Context.getInstance().initialize(new Properties(), nodeConfiguration);

        NodeStore nodeStore = new NodeStore();
        nodeStore.init();
        nodeStore.store("bob", "dataset_1", "entity_1", 1000L, 123456L, createStrand("9xj", 1391216400000L,
                1391216400100L, 1.0, 2.0));
        nodeStore.store("bob", "dataset_1", "entity_2", 1000L, 123456L, createStrand("9xj", 1391216400100L,
                1391216400200L, 1.0, 2.0));
        nodeStore.store("bob", "dataset_2", "entity_2", 1000L, 123456L, createStrand("9xj", 1391216400100L,
                1391216400200L, 1.0, 2.0));
        nodeStore.endSession("dataset_1", "bob", 1000L, 123456L);


        // simulate a restarted node store by starting a NodeStore by pointing to the same commit log
        NodeStore nodeStoreRestarted = new NodeStore();
        nodeStoreRestarted.init();
        assertEquals(2, nodeStoreRestarted.entityStoreMap.size());
        assertEquals(2, nodeStoreRestarted.entityStoreMap.get("dataset_1").size());
        assertTrue(nodeStoreRestarted.entityStoreMap.get("dataset_1").containsKey("entity_1"));
        assertTrue(nodeStoreRestarted.entityStoreMap.get("dataset_1").containsKey("entity_2"));
        assertEquals(1, nodeStoreRestarted.entityStoreMap.get("dataset_2").size());
        assertTrue(nodeStoreRestarted.entityStoreMap.get("dataset_2").containsKey("entity_2"));
    }

    @Test
    void testIngestionSession(){
        IngestionSession session1 = new IngestionSession("bob", 1234L, 1000L);
        IngestionSession session2 = new IngestionSession("bob", 12344L, 1000L);
        IngestionSession session3 = new IngestionSession("bob", 1234L, 1001L);

        assertEquals(session1, session2); // same session id
        assertNotEquals(session1, session3); // different session ids
    }
}
