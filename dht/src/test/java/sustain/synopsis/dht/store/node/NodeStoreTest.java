package sustain.synopsis.dht.store.node;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.query.QueryException;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.workers.WriterPool;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.serializeStrand;

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

    @Mock
    EntityStore entityStoreMock1;

    @Mock
    EntityStore entityStoreMock2;

    @Test
    void testFreshStart() throws StorageException, IOException {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
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
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(true);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());

        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        nodeStore.init();
        assertTrue(nodeStore.entityStoreMap.isEmpty());
        assertTrue(nodeStore.validatedSessions.isEmpty());
        nodeStore.store("dataset_1", "entity_1", 1000L, new StrandStorageKey(1391216400000L, 1391216400100L),
                        new StrandStorageValue(
                                serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertTrue(nodeStore.entityStoreMap.containsKey("dataset_1"));
        assertTrue(nodeStore.entityStoreMap.get("dataset_1").containsKey("entity_1"));
        assertTrue(nodeStore.validatedSessions.containsKey(1000L));

        String entityCommitLogPath = metadataStoreDir.getAbsolutePath() + File.separator + "entity_1_metadata.slog";
        Mockito.verify(sessionValidatorMock, Mockito.times(1)).validate("dataset_1", 1000L);
        Mockito.verify(loggerMock, Mockito.times(1))
               .append(new CreateEntityStoreActivity("dataset_1", "entity_1").serialize());
        // we can verify if there is a log written into the entity store log to make sure the store request is made to
        // to the entity store
        // log appenders are initialized
        File entityCommitLog = new File(entityCommitLogPath);
        assertTrue(entityCommitLog.exists());

        // add more data for the same entity store
        Strand strand = createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0);
        nodeStore.store("bob", "dataset_1", "entity_1", 1000L, 123456L,
                        new StrandStorageKey(strand.getFromTimeStamp(), strand.getToTimestamp()),
                        new StrandStorageValue(serializeStrand(strand)));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertEquals(1, nodeStore.entityStoreMap.get("dataset_1").size());

        // add data to a different entity store
        strand = createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0);
        nodeStore.store("bob", "dataset_1", "entity_2", 1000L, 123456L,
                        new StrandStorageKey(strand.getFromTimeStamp(), strand.getToTimestamp()),
                        new StrandStorageValue(serializeStrand(strand)));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertEquals(2, nodeStore.entityStoreMap.get("dataset_1").size());
        assertTrue(nodeStore.entityStoreMap.get("dataset_1").containsKey("entity_2"));

        // add data to a different dataset
        strand = createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0);
        nodeStore.store("bob", "dataset_2", "entity_2", 1000L, 123456L,
                        new StrandStorageKey(strand.getFromTimeStamp(), strand.getToTimestamp()),
                        new StrandStorageValue(serializeStrand(strand)));
        assertEquals(2, nodeStore.entityStoreMap.size());
        assertTrue(nodeStore.entityStoreMap.containsKey("dataset_2"));

        // test end_session
        WriterPool writerPool = new WriterPool(2);
        List<CompletableFuture<Boolean>> endSessionFutures = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(2, endSessionFutures.size());
        CompletableFuture.allOf(endSessionFutures.toArray(new CompletableFuture[0])).thenApply(
                future -> endSessionFutures.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertTrue);

        List<CompletableFuture<Boolean>> futures = nodeStore.endSession("dataset_2", 1000L, writerPool);
        Assertions.assertEquals(1, futures.size());
        futures.get(0).thenAccept(Assertions::assertTrue);
    }

    @Test
    void testRestart() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        Mockito.when(sessionValidatorMock.validate("dataset_2", 1001L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "alice", 7896L));
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
        Context.getInstance().initialize(nodeConfiguration);

        NodeStore nodeStore = new NodeStore();
        nodeStore.init();
        nodeStore.store("bob", "dataset_1", "entity_1", 1000L, 123456L,
                        new StrandStorageKey(1391216400000L, 1391216400100L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        nodeStore.store("bob", "dataset_1", "entity_2", 1000L, 123456L,
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.store("bob", "dataset_2", "entity_2", 1001L, 123456L,
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.endEntityStoreSession("dataset_1", "entity_1", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "entity_2", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_2", "entity_2", "bob", 1000L, 123456L);


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
    void testEndSession() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
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
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(true);

        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        nodeStore.init();
        WriterPool writerPool = new WriterPool(2);

        // valid session, but no such dataset
        nodeStore.validatedSessions.clear();
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        List<CompletableFuture<Boolean>> futures1 = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(1, futures1.size());
        CompletableFuture.allOf(futures1.toArray(new CompletableFuture[0])).thenApply(
                future -> futures1.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertTrue);

        // invalid session
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(false, "bob", 12345L));
        List<CompletableFuture<Boolean>> futures2 = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(1, futures2.size());
        CompletableFuture.allOf(futures2.toArray(new CompletableFuture[0])).thenApply(
                future -> futures2.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertFalse);

        // add mock entity stores
        // reset the session validator validate the session as valid
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        IngestionSession session = new IngestionSession("bob", 12345L, 1000L);
        Mockito.when(entityStoreMock1.endSession(session)).thenReturn(true);
        Mockito.when(entityStoreMock2.endSession(session)).thenReturn(true);
        Trie<String, EntityStore> entityStores = new PatriciaTrie<>();
        entityStores.put("entity_1", entityStoreMock1);
        entityStores.put("entity_2", entityStoreMock2);
        nodeStore.entityStoreMap.put("dataset_1", entityStores);
        List<CompletableFuture<Boolean>> futures3 = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(2, futures3.size());
        CompletableFuture.allOf(futures2.toArray(new CompletableFuture[0])).thenApply(
                future -> futures3.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertTrue);
        Mockito.verify(entityStoreMock1, Mockito.times(1)).endSession(session);
        Mockito.verify(entityStoreMock2, Mockito.times(1)).endSession(session);
    }

    @Test
    void testIngestionSession() {
        IngestionSession session1 = new IngestionSession("bob", 1234L, 1000L);
        IngestionSession session2 = new IngestionSession("bob", 12344L, 1000L);
        IngestionSession session3 = new IngestionSession("bob", 1234L, 1001L);

        assertEquals(session1, session2); // same session id
        assertNotEquals(session1, session3); // different session ids
    }

    @Test
    void testDiskManagerInitFail() {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
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
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(false);
        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        Assertions.assertThrows(StorageException.class, nodeStore::init);
    }

    @Test
    void testGetMatchingEntityStoresForCorrectDataset() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        Predicate predicate = Predicate.newBuilder().setStringValue("8qr").build();
        TargetQueryRequest req =
                TargetQueryRequest.newBuilder().setDataset("dataset_2").addSpatialScope(predicate).build();
        Set<EntityStore> matchingStores = nodeStore.getMatchingEntityStores(req);
        Assertions.assertEquals(1, matchingStores.size());
        Assertions.assertEquals("8qr", matchingStores.iterator().next().getEntityId());
    }

    @Test
    void testGetMatchingEntityStoresForInvalidDataset() throws StorageException, IOException {
        NodeStore nodeStore = prepareTestNodeStore();
        Predicate predicate = Predicate.newBuilder().setStringValue("8qr").build();
        TargetQueryRequest finalReq =
                TargetQueryRequest.newBuilder().setDataset("non_existing_dataset").addSpatialScope(predicate).build();
        Assertions.assertThrows(QueryException.class, () -> {
            nodeStore.getMatchingEntityStores(finalReq);
        });
    }

    @Test
    void testGetMatchingEntityStoresForZeroSpatialScope() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        TargetQueryRequest emptyReq = TargetQueryRequest.newBuilder().setDataset("dataset_1").build();
        Assertions.assertTrue(nodeStore.getMatchingEntityStores(emptyReq).isEmpty());
    }

    @Test
    void testGetMatchingEntityStoresForInvalidSpatialScope() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        Predicate predicate = Predicate.newBuilder().setStringValue("9yyi").build();
        TargetQueryRequest req =
                TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertTrue(nodeStore.getMatchingEntityStores(req).isEmpty());
    }

    @Test
    void testGetMatchingEntityStoresForPrefixMatching() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        // use a single character - should match with all entity stores
        Predicate predicate = Predicate.newBuilder().setStringValue("9").build();
        TargetQueryRequest req =
                TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5", "9x")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));

        // three characters
        predicate = Predicate.newBuilder().setStringValue("9xi").build();
        req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));

        // exact match
        predicate = Predicate.newBuilder().setStringValue("9xi5").build();
        req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertEquals(new HashSet<>(Collections.singletonList("9xi5")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));
    }

    @Test
    void testGetMatchingEntityStoresForPrefixMatchingWithMultipleSpatialScopes()
            throws IOException, StorageException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        // non-overlapping scopes
        Predicate predicate1 = Predicate.newBuilder().setStringValue("9xi").build();
        Predicate predicate2 = Predicate.newBuilder().setStringValue("8x").build();
        TargetQueryRequest req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate1).
                addSpatialScope(predicate2).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5", "8x")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));

        // overallping scopes
        predicate2 = Predicate.newBuilder().setStringValue("9xi5").build();
        req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate1).
                addSpatialScope(predicate2).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));
    }

    private NodeStore prepareTestNodeStore() throws StorageException, IOException {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
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
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(true);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());

        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        nodeStore.init();
        // dataset_1
        nodeStore.store("bob", "dataset_1", "9xi", 1000L, 123456L, new StrandStorageKey(1391216400000L, 1391216400100L),
                        new StrandStorageValue(
                                serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        nodeStore
                .store("bob", "dataset_1", "9xi5", 1000L, 123456L, new StrandStorageKey(1391216400100L, 1391216400200L),
                       new StrandStorageValue(
                               serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.store("bob", "dataset_1", "9x", 1000L, 123456L, new StrandStorageKey(1391216400100L, 1391216400200L),
                        new StrandStorageValue(
                                serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.store("bob", "dataset_1", "8x", 1000L, 123456L, new StrandStorageKey(1391216400100L, 1391216400200L),
                        new StrandStorageValue(
                                serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.endEntityStoreSession("dataset_1", "9xi", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "9xi5", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "9x", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "8x", "bob", 1000L, 123456L);

        // dataset_2
        nodeStore.store("bob", "dataset_2", "8qr", 1001L, 123456L, new StrandStorageKey(1391216400000L, 1391216400100L),
                        new StrandStorageValue(
                                serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        nodeStore.endEntityStoreSession("dataset_2", "8qr", "bob", 1001L, 123456L);
        return nodeStore;
    }
}
