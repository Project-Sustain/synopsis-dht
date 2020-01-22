package sustain.synopsis.dht.store.entity;

import junit.framework.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.IngestionSession;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.journal.JournalingException;
import sustain.synopsis.dht.store.entity.journal.activity.EndSessionActivity;
import sustain.synopsis.dht.store.entity.journal.activity.IncSeqIdActivity;
import sustain.synopsis.dht.store.entity.journal.activity.SerializeSSTableActivity;
import sustain.synopsis.dht.store.entity.journal.activity.StartSessionActivity;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EntityStoreJournalTest {

    @TempDir
    File tempDir;

    @Test
    void testGetJournalFilePath(){
        EntityStoreJournal metaStore = new EntityStoreJournal("entity1", "/tmp");
        Assertions.assertEquals("/tmp" + File.separator + "entity1_metadata.slog",
                metaStore.getJournalFilePath());
    }

    @Test
    void testValidateSerializeSSTableActivity() throws IOException {
        Map<Long, IngestionSession> sessions = new HashMap<>();
        sessions.put(1000L, new IngestionSession("bob", 123L, 1000L));
        sessions.put(1001L, new IngestionSession("alice", 678L, 1001L));

        EntityStoreJournal metaStore = new EntityStoreJournal("", "");

        // invalid session id
        SerializeSSTableActivity activity = new SerializeSSTableActivity(1002L, new Metadata<>());
        Assertions.assertFalse(metaStore.validateSerializeSSTableActivity(sessions, activity));

        // non-existing path
        String path = tempDir.getAbsolutePath() + File.separator + "non-existing-path";
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        metadata.setMin(new StrandStorageKey(10L, 15L));
        metadata.setMax(new StrandStorageKey(20L, 25L));
        metadata.setPath(path);
        activity = new SerializeSSTableActivity(1000L, metadata);
        Assertions.assertFalse(metaStore.validateSerializeSSTableActivity(sessions, activity));

        // ssTable file points to a directory
        metadata.setPath(tempDir.getAbsolutePath());
        activity = new SerializeSSTableActivity(1000L, metadata);
        Assertions.assertFalse(metaStore.validateSerializeSSTableActivity(sessions, activity));

        // valid entry
        File f = new File(tempDir.getAbsolutePath() + File.separator + "test.file");
        f.createNewFile();
        Assertions.assertTrue(f.exists());
        metadata.setPath(f.getAbsolutePath());
        activity = new SerializeSSTableActivity(1000L, metadata);
        Assertions.assertTrue(metaStore.validateSerializeSSTableActivity(sessions, activity));
    }

    @Test
    void testValidateEndSessionActivity() {
        Map<Long, IngestionSession> sessions = new HashMap<>();
        sessions.put(1000L, new IngestionSession("bob", 123L, 1000L));

        EntityStoreJournal metaStore = new EntityStoreJournal("", "");
        // invalid session id
        Assertions.assertFalse(metaStore.validateEndSessionActivity(sessions, new EndSessionActivity(1001L)));
        // valid session
        Assertions.assertTrue(metaStore.validateEndSessionActivity(sessions, new EndSessionActivity(1000L)));
    }

    @Test
    void testParseJournal() throws IOException, StorageException, JournalingException {
        Logger logger = new Logger(tempDir.getAbsolutePath() + File.separator + "test.slog");

        StartSessionActivity startSessionActivity = new StartSessionActivity("bob", 123L, 1000L);
        logger.append(startSessionActivity.serialize());

        File f = new File(tempDir.getAbsolutePath() + File.separator + "test.file");
        f.createNewFile();
        Metadata<StrandStorageKey> metadata1 = new Metadata<>();
        metadata1.setMin(new StrandStorageKey(10L, 15L));
        metadata1.setMax(new StrandStorageKey(20L, 25L));
        metadata1.setPath(f.getAbsolutePath());
        SerializeSSTableActivity serializeSSTableActivity = new SerializeSSTableActivity(1000L, metadata1);
        logger.append(serializeSSTableActivity.serialize());
        logger.append(new IncSeqIdActivity(1).serialize());

        metadata1.setMin(new StrandStorageKey(30L, 35L));
        metadata1.setMin(new StrandStorageKey(50L, 55L));
        metadata1.setPath(f.getAbsolutePath()); // use a valid path to pass the validation
        logger.append(serializeSSTableActivity.serialize());

        logger.append(new EndSessionActivity(1000L).serialize());

        logger.append(new StartSessionActivity("alice", 567L, 2000L).serialize());
        logger.append(new EndSessionActivity(2000L).serialize());
        logger.append(new IncSeqIdActivity(2).serialize());
        logger.close();

        EntityStoreJournal metaStore = new EntityStoreJournal("", "");
        metaStore.parseJournal(logger.iterator());
        Map<Long, IngestionSession> sessions = metaStore.getSessions();

        Assertions.assertEquals(2, sessions.size());
        IngestionSession session1 = sessions.get(1000L);
        Assertions.assertNotNull(session1);
        Assertions.assertEquals(2, session1.getSerializedSSTables().size());
        Assertions.assertTrue(session1.isComplete());

        IngestionSession session2 = sessions.get(2000L);
        Assertions.assertNotNull(session2);
        Assertions.assertTrue(session2.isComplete());
        Assert.assertEquals(2, metaStore.getSequenceId());
    }

    @Test
    void testParsingInvalidEvents() throws IOException, StorageException {
        Logger logger = new Logger(tempDir.getAbsolutePath() + File.separator + "test.slog");
        StartSessionActivity startSessionActivity = new StartSessionActivity("bob", 123L, 1000L);
        logger.append(startSessionActivity.serialize());

        File f = new File(tempDir.getAbsolutePath() + File.separator + "test.file"); // does not exist
        Metadata<StrandStorageKey> metadata1 = new Metadata<>();
        metadata1.setMin(new StrandStorageKey(10L, 15L));
        metadata1.setMax(new StrandStorageKey(20L, 25L));
        metadata1.setPath(f.getAbsolutePath());
        SerializeSSTableActivity serializeSSTableActivity = new SerializeSSTableActivity(1000L, metadata1);
        logger.append(serializeSSTableActivity.serialize());

        logger.append(new EndSessionActivity(1000L).serialize());
        logger.close();

        EntityStoreJournal metaStore = new EntityStoreJournal("", "");
        Assertions.assertThrows(JournalingException.class, () -> metaStore.parseJournal(logger.iterator()));

        Logger logger2 = new Logger(tempDir.getAbsolutePath() + File.separator + "test2.slog");
        logger2.append(new EndSessionActivity(2000L).serialize());
        Assertions.assertThrows(JournalingException.class, () -> metaStore.parseJournal(logger2.iterator()));
    }
}
