package sustain.synopsis.dht.store.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.services.query.Interval;
import sustain.synopsis.dht.services.query.MatchedSSTable;
import sustain.synopsis.dht.services.query.QueryException;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.serializeStrand;

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
        EntityStore store =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 1024, 50, diskManagerMock);
        store.init();
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        String returned = store.getSSTableOutputPath(key1, key2, storageDir.getAbsolutePath(), 0);
        String expected = storageDir.getAbsolutePath() + File.separator + "dataset1_9xj_" + key1.toString() + "_" + key2
                .toString() + "_0.sd";
        assertEquals(expected, returned);
    }

    @Test
    void testToSSTable() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        EntityStore entityStore =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 1024, 50, diskManagerMock);
        entityStore.init();
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        entityStore.store(session, key1, value1);
        entityStore.store(session, key2, value2);
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        entityStore.toSSTable(session, diskManagerMock, metadata);

        File serializedSSTable = new File(
                storageDir.getAbsolutePath() + File.separator + "dataset1_9xj" + "_" + key1.toString() + "_" + key2
                        .toString() + "_0.sd");
        assertTrue(serializedSSTable.exists());
        assertTrue(serializedSSTable.isFile());
        assertTrue(serializedSSTable.length() > 0);
        assertEquals(serializedSSTable.getAbsolutePath(), metadata.getPath());
    }

    @Test
    void testStoreStrand() throws StorageException, IOException {     // testing the entire flow
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        Mockito.when(entityStoreJournalMock.init()).thenReturn(true);
        Mockito.when(entityStoreJournalMock.getSequenceId()).thenReturn(0);
        Mockito.when(entityStoreJournalMock.getMetadata()).thenReturn(new ArrayList<>());
        EntityStore entityStore = new EntityStore("dataset_1", "9xj", entityStoreJournalMock, 80, 20, diskManagerMock);
        entityStore.init();
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).init();

        // start a new session
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).startSession(session);

        assertEquals(1, entityStore.activeSessions.size());
        assertEquals(1, entityStore.activeMetadata.size());

        // size of key and value used here - 187 bytes
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key1, value1); // this should fill up the memTable
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key2, value2);
        // storing 2 strands should fill out the memTable
        Mockito.verify(diskManagerMock, Mockito.times(1)).allocate(Mockito.anyLong());
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).addSerializedSSTable(Mockito.any(), Mockito.any());
        assertEquals(0, entityStore.queryableMetadata.size());
        Metadata<StrandStorageKey> metadata = entityStore.activeMetadata.get(session).get(0);
        assertEquals(key1, metadata.getMin());
        assertEquals(key2, metadata.getMax());
        assertEquals(entityStore.getSSTableOutputPath(key1, key2, storageDir.getAbsolutePath(), 0), metadata.getPath());

        // add more data
        StrandStorageKey key3 = new StrandStorageKey(1391216400200L, 1391216400300L);
        StrandStorageValue value3 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key3, value3);

        // end session
        Assertions.assertTrue(entityStore.endSession(session));
        // at this point, there should be two calls to the disk_manager#allcate method
        Mockito.verify(diskManagerMock, Mockito.times(2)).allocate(Mockito.anyLong());
        Mockito.verify(entityStoreJournalMock, Mockito.times(2)).addSerializedSSTable(Mockito.any(), Mockito.any());
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).endSession(session);
        assertEquals(2, entityStore.queryableMetadata.size());
        assertEquals(0, entityStore.activeMetadata.size());
        assertEquals(0, entityStore.activeSessions.size());

        // try to an non-existing session
        Assertions.assertTrue(entityStore.endSession(new IngestionSession("alice", System.currentTimeMillis(), 100)));
    }

    @Test
    void testNodeRestart() throws IOException, StorageException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        EntityStore entityStore =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 80, 20, diskManagerMock);
        entityStore.init();
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key1, value1); // this should fill up the memTable
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key2, value2);
        StrandStorageKey key3 = new StrandStorageKey(1391216400200L, 1391216400300L);
        StrandStorageValue value3 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key3, value3);
        // end session
        Assertions.assertTrue(entityStore.endSession(session));

        // Simulate a node restart
        EntityStore restartedEntityStore =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 80, 20, diskManagerMock);
        restartedEntityStore.init();
        // there were two SSTables written before. So the sequence ID should start from 2.
        assertEquals(2, restartedEntityStore.sequenceId.get());
        assertEquals(2, restartedEntityStore.queryableMetadata.size());

        // write some more data
        session = new IngestionSession("bob", System.currentTimeMillis(), 1);
        StrandStorageKey key4 = new StrandStorageKey(1391216400300L, 1391216400400L);
        StrandStorageValue value4 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400300L, 1391216400400L, 1.0, 2.0)));
        restartedEntityStore.store(session, key4, value4); // this should fill up the memTable
        StrandStorageKey key5 = new StrandStorageKey(1391216400400L, 1391216400500L);
        StrandStorageValue value5 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400400L, 1391216400500L, 1.0, 2.0)));
        restartedEntityStore.store(session, key5, value5); // this should fill up the memTable

        assertEquals(3, restartedEntityStore.sequenceId.get());
        assertEquals(2, restartedEntityStore.queryableMetadata.size());
        assertEquals(1, restartedEntityStore.activeSessions.size());
        assertEquals(1, restartedEntityStore.activeMetadata.size());
        Assertions.assertTrue(restartedEntityStore.endSession(session));
        assertEquals(3, restartedEntityStore.queryableMetadata.size());
    }

    @Test
    void testQuery() throws StorageException, IOException, QueryException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        Mockito.when(entityStoreJournalMock.init()).thenReturn(true);
        Mockito.when(entityStoreJournalMock.getSequenceId()).thenReturn(0);
        Mockito.when(entityStoreJournalMock.getMetadata()).thenReturn(new ArrayList<>());
        EntityStore entityStore = new EntityStore("dataset1", "9xj", entityStoreJournalMock, 80, 20, diskManagerMock);
        entityStore.init();

        // start a new session
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);

        // there are no queryable sessions at the moment
        Expression exp = Expression.newBuilder().setPredicate1(
                Predicate.newBuilder().setIntegerValue(1750).setComparisonOp(Predicate.ComparisonOperator.LESS_THAN)
                         .build()).setCombineOp(Expression.CombineOperator.OR).setPredicate2(
                Predicate.newBuilder().setIntegerValue(1800).setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN)
                         .build()).build();
        List<MatchedSSTable> results = entityStore.temporalQuery(exp);
        Assertions.assertTrue(results.isEmpty());

        // size of key and value used here - (52 + 16) bytes
        // storing 2 strands should fill out the memTable
        StrandStorageKey key1 = new StrandStorageKey(1000, 1500);
        StrandStorageValue value1 = new StrandStorageValue(serializeStrand(createStrand("9xj", 1000, 1500, 1.0, 2.0)));
        entityStore.store(session, key1, value1);
        StrandStorageKey key2 = new StrandStorageKey(1500, 2000);
        StrandStorageValue value2 = new StrandStorageValue(serializeStrand(createStrand("9xj", 1500, 2000, 1.0, 2.0)));
        entityStore.store(session, key2, value2);
        // add more data
        StrandStorageKey key3 = new StrandStorageKey(2000, 2500);
        StrandStorageValue value3 = new StrandStorageValue(serializeStrand(createStrand("9xj", 2000, 2500, 1.0, 2.0)));
        entityStore.store(session, key3, value3);
        // end session
        Assertions.assertTrue(entityStore.endSession(session));
        Assertions.assertEquals(2, entityStore.queryableMetadata.size());

        results = entityStore.temporalQuery(exp);
        Assertions.assertEquals(2, results.size());
        // sort the results by the strand timestamps, so that we can check individual values
        results.sort(Comparator.comparing(o -> o.getMetadata().getMin()));
        Assertions.assertEquals(2, results.get(0).getMatchedIntervals().size());
        List<Interval> intervals = results.get(0).getMatchedIntervals();
        intervals.sort(Comparator.comparingLong(Interval::getFrom));
        Assertions.assertEquals(Arrays.asList(new Interval(1000, 1750), new Interval(1801, 2500)), intervals);

        Assertions.assertEquals(1, results.get(1).getMatchedIntervals().size());
        Assertions.assertEquals(Collections.singletonList(new Interval(1801, 2500)),
                                results.get(1).getMatchedIntervals());
    }
}

