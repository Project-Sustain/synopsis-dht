package sustain.synopsis.dht.store.entity;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.storage.lsmtree.ChecksumGenerator;
import sustain.synopsis.storage.lsmtree.MemTable;
import sustain.synopsis.storage.lsmtree.Metadata;
import sustain.synopsis.storage.lsmtree.SSTableWriter;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class EntityStore {
    private final Logger logger = Logger.getLogger(EntityStore.class);
    private final String entityId;
    public final long blockSize;
    /**
     * There can be multiple active ingestion sessions at a given time for a single entity.
     */
    private Map<IngestionSession, MemTable<StrandStorageKey, StrandStorageValue>> activeSessions;
    private Map<IngestionSession, List<Metadata<StrandStorageKey>>> activeMetadata;
    private DiskManager diskManager;
    private int sequenceId;
    // todo: these should be read from the config
    private BlockCompressor compressor;
    private ChecksumGenerator checksumGenerator;
    private EntityStoreJournal entityStoreJournal;
    private long memTableSize;
    private List<Metadata<StrandStorageKey>> queryiableMetadata;

    public EntityStore(String entityId, String metadataDir, long memTableSize, long blockSize) {
        this.entityId = entityId;
        this.blockSize = blockSize;
        this.activeSessions = new HashMap<>();
        this.activeMetadata = new HashMap<>();
        this.compressor = new LZ4BlockCompressor();
        this.memTableSize = memTableSize;
        this.entityStoreJournal = new EntityStoreJournal(entityId, metadataDir);
        this.queryiableMetadata = new ArrayList<>();
    }

    public boolean init() throws StorageException {
        return init(this.diskManager);
    }

    // used for unit testing by injecting the disk manager
    public boolean init(DiskManager diskManager) throws StorageException {
        try {
            this.checksumGenerator = new ChecksumGenerator();
            this.diskManager = diskManager;
            boolean success = this.entityStoreJournal.init();
            if (!success) {
                return false;
            }
            this.sequenceId = entityStoreJournal.getSequenceId();
            List<Metadata<StrandStorageKey>> metadataList = entityStoreJournal.getMetadata();
            this.queryiableMetadata =
                    metadataList.stream().filter(Metadata::isSessionComplete).collect(Collectors.toList());
            // todo: how to handle sessions  active during the node crash/shutdown?
            // todo: populate activeSessions and activeMetadata
        } catch (ChecksumGenerator.ChecksumError checksumError) {
            logger.error(checksumError.getMessage(), checksumError);
            throw new StorageException(checksumError.getMessage(), checksumError);
        }
        return true;
    }

    public void startSession(IngestionSession session) throws IOException, StorageException {
        if (activeSessions.containsKey(session)) {
            logger.warn("Trying to initiate already existing session. Session id: " + session.getSessionId());
            return;
        }
        try {
            entityStoreJournal.startSession(session); // log first
            activeSessions.put(session, new MemTable<>(memTableSize));
            activeMetadata.put(session, new ArrayList<>());
        } catch (IOException | StorageException e) {
            logger.error("Error initializing the session. ", e);
            throw e;
        }
    }

    public boolean store(IngestionSession session, StrandStorageKey key, StrandStorageValue value) {
        // todo: data should be first written to a WAL
        try {
            if (!activeSessions.containsKey(session)) {
                startSession(session);
            }
            MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
            boolean isMemTableFull = memTable.add(key, value);
            if (isMemTableFull) {
                purgeMemTable(session, memTable);
            }
        } catch (IOException | StorageException e) {
            logger.error("Error storing the strand.", e);
            return false;
        }
        return true;
    }

    private void purgeMemTable(IngestionSession session, MemTable<StrandStorageKey, StrandStorageValue> memTable) throws IOException, StorageException {
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        metadata.setSessionId(session.getSessionId());
        metadata.setUser(session.getIngestionUser());
        metadata.setSessionStartTS(session.getSessionStartTS());
        entityStoreJournal.addSerializedSSTable(session, metadata);
        toSSTable(session, diskManager, metadata);
        activeMetadata.get(session).add(metadata);
        memTable.clear();
    }

    public void endSession(IngestionSession session) throws StorageException, IOException {
        if (!activeSessions.containsKey(session)) {
            return;
        }
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        if (memTable.getEntryCount() > 0) {
            purgeMemTable(session, memTable);
        }
        entityStoreJournal.endSession(session);
        queryiableMetadata.addAll(activeMetadata.get(session));
        activeSessions.remove(session);
        activeMetadata.remove(session);
    }

    // we inject a disk manager instance for unit testing purposes
    public void toSSTable(IngestionSession session, DiskManager diskManager, Metadata<StrandStorageKey> metadata) throws StorageException, IOException {
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        memTable.setReadOnly();
        // we throw the exception for the time being - verify once the upper layer is implemented
        String dir = diskManager.allocate(memTable.getEstimatedSize());
        String storagePath = getSSTableOutputPath(memTable.getFirstKey(), memTable.getLastKey(), dir);
        try (FileOutputStream fos = new FileOutputStream(storagePath); DataOutputStream dos =
                new DataOutputStream(fos)) {
            SSTableWriter<StrandStorageKey, StrandStorageValue> ssTableWriter = new SSTableWriter<>(blockSize,
                    Collections.singletonList(memTable.getIterator()));
            ssTableWriter.serialize(dos, metadata, compressor, checksumGenerator);
            dos.flush();
            fos.flush();
            metadata.setPath(storagePath);
        } catch (IOException e) {
            logger.error("Error converting memTable to SSTable.", e);
            throw new StorageException("Error converting memTable to SSTable.", e);
        }
    }

    public String getSSTableOutputPath(StrandStorageKey firstKey, StrandStorageKey lastKey, String path) throws IOException, StorageException {
        entityStoreJournal.incrementSequenceId(++sequenceId);
        return path + File.separator + entityId + "_" + firstKey + "_" + lastKey + "_" + sequenceId + ".sd";
    }

    public String getJournalFilePath() {
        return entityStoreJournal.getJournalFilePath();
    }
}
