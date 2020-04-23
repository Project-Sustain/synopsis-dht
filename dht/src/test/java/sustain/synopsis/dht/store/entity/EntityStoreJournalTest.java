package sustain.synopsis.dht.store.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sustain.synopsis.dht.journal.Logger;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EntityStoreJournalTest {

    @TempDir
    File tempDir;

    @Test
    void testGetJournalFilePath() {
        EntityStoreJournal metaStore = new EntityStoreJournal("dataset1", "entity1", "/tmp");
        Assertions.assertEquals("/tmp" + File.separator + "dataset1_entity1_metadata.slog",
                                metaStore.getJournalFilePath());
    }

    @Test
    void testValidateSerializeSSTableActivity() throws IOException {
        Map<Long, List<Metadata<StrandStorageKey>>> metadataList = new HashMap<>();
        metadataList.put(1000L, new ArrayList<>());
        metadataList.put(1001L, new ArrayList<>());

        EntityStoreJournal metaStore = new EntityStoreJournal("", "", "");

        // invalid session id
        SerializeSSTableActivity activity = new SerializeSSTableActivity(1002L, new Metadata<>());
        Assertions.assertFalse(metaStore.validateSerializeSSTableActivity(metadataList, activity));

        // non-existing path
        String path = tempDir.getAbsolutePath() + File.separator + "non-existing-path";
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        metadata.setMin(new StrandStorageKey(10L, 15L));
        metadata.setMax(new StrandStorageKey(20L, 25L));
        metadata.setPath(path);
        activity = new SerializeSSTableActivity(1000L, metadata);
        Assertions.assertFalse(metaStore.validateSerializeSSTableActivity(metadataList, activity));

        // ssTable file points to a directory
        metadata.setPath(tempDir.getAbsolutePath());
        activity = new SerializeSSTableActivity(1000L, metadata);
        Assertions.assertFalse(metaStore.validateSerializeSSTableActivity(metadataList, activity));

        // valid entry
        File f = new File(tempDir.getAbsolutePath() + File.separator + "test.file");
        f.createNewFile();
        Assertions.assertTrue(f.exists());
        metadata.setPath(f.getAbsolutePath());
        activity = new SerializeSSTableActivity(1000L, metadata);
        Assertions.assertTrue(metaStore.validateSerializeSSTableActivity(metadataList, activity));
    }

    @Test
    void testValidateEndSessionActivity() {
        Map<Long, List<Metadata<StrandStorageKey>>> sessions = new HashMap<>();
        sessions.put(1000L, new ArrayList<>());

        EntityStoreJournal metaStore = new EntityStoreJournal("", "", "");
        // invalid session id
        Assertions.assertFalse(metaStore.validateEndSessionActivity(sessions, new EndSessionActivity(1001L)));
        // valid session
        Assertions.assertTrue(metaStore.validateEndSessionActivity(sessions, new EndSessionActivity(1000L)));
    }

    @Test
    void testParseJournal() throws IOException, StorageException, JournalingException {
        Logger logger = new Logger(tempDir.getAbsolutePath() + File.separator + "test.slog");

        StartSessionActivity startSessionActivity = new StartSessionActivity(1000L);
        logger.append(startSessionActivity.serialize());

        File f = new File(tempDir.getAbsolutePath() + File.separator + "test.file");
        f.createNewFile();
        Metadata<StrandStorageKey> metadata1 = new Metadata<>();
        metadata1.setMin(new StrandStorageKey(10L, 15L));
        metadata1.setMax(new StrandStorageKey(20L, 25L));
        metadata1.setPath(f.getAbsolutePath()); // use a valid file path
        metadata1.setSessionId(1000L);
        metadata1.setUser("bob");
        SerializeSSTableActivity serializeSSTableActivity = new SerializeSSTableActivity(1000L, metadata1);
        logger.append(serializeSSTableActivity.serialize());
        logger.append(new IncSeqIdActivity(1).serialize());

        metadata1.setMin(new StrandStorageKey(30L, 35L));
        metadata1.setMin(new StrandStorageKey(50L, 55L));
        logger.append(serializeSSTableActivity.serialize());

        logger.append(new EndSessionActivity(1000L).serialize());

        logger.append(new StartSessionActivity(2000L).serialize());
        logger.append(new EndSessionActivity(2000L).serialize());
        logger.append(new IncSeqIdActivity(2).serialize());
        logger.close();

        EntityStoreJournal metaStore = new EntityStoreJournal("","", "");
        metaStore.parseJournal(logger.iterator());
        List<Metadata<StrandStorageKey>> metadataList = metaStore.getMetadata();
        Assertions.assertEquals(2, metadataList.size());
        Assertions.assertEquals(2, metaStore.getSequenceId());
    }

    @Test
    void testParsingInvalidEvents() throws IOException, StorageException {
        Logger logger = new Logger(tempDir.getAbsolutePath() + File.separator + "test.slog");
        StartSessionActivity startSessionActivity = new StartSessionActivity(1000L);
        logger.append(startSessionActivity.serialize());

        File f = new File(tempDir.getAbsolutePath() + File.separator + "test.file"); // does not exist
        Metadata<StrandStorageKey> metadata1 = new Metadata<>();
        metadata1.setMin(new StrandStorageKey(10L, 15L));
        metadata1.setMax(new StrandStorageKey(20L, 25L));
        metadata1.setPath(f.getAbsolutePath());
        metadata1.setSessionId(1000L);
        metadata1.setUser("bob");
        metadata1.setSessionStartTS(System.currentTimeMillis());
        SerializeSSTableActivity serializeSSTableActivity = new SerializeSSTableActivity(1000L, metadata1);
        logger.append(serializeSSTableActivity.serialize());

        logger.append(new EndSessionActivity(1000L).serialize());
        logger.close();

        EntityStoreJournal metaStore = new EntityStoreJournal("", "", "");
        Assertions.assertThrows(JournalingException.class, () -> metaStore.parseJournal(logger.iterator()));

        Logger logger2 = new Logger(tempDir.getAbsolutePath() + File.separator + "test2.slog");
        logger2.append(new EndSessionActivity(2000L).serialize());
        Assertions.assertThrows(JournalingException.class, () -> metaStore.parseJournal(logger2.iterator()));
    }
}
