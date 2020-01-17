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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class EntityStore {
    private final Logger logger = Logger.getLogger(EntityStore.class);
    private final String entityId;
    private final String metadataDir;
    public final long blockSize;
    private Map<IngestionSession, MemTable<StrandStorageKey, StrandStorageValue>> activeSessions;
    private DiskManager diskManager;
    private int sequenceId;
    // todo: these should be read from the config
    private BlockCompressor compressor;
    private ChecksumGenerator checksumGenerator;
    private EntityMetaStore metaStore;
    private long memTableSize;

    public EntityStore(String entityId, String metadataDir, long memTableSize, long blockSize) {
        this.entityId = entityId;
        this.metadataDir = metadataDir;
        this.blockSize = blockSize;
        this.activeSessions = new HashMap<>();
        this.sequenceId = 0;
        this.compressor = new LZ4BlockCompressor();
        this.memTableSize = memTableSize;
        this.metaStore = new EntityMetaStore(entityId, metadataDir);
    }

    public boolean init(CountDownLatch latch) throws StorageException {
        this.diskManager = DiskManager.getInstance();
        return init(latch, this.diskManager);
    }

    // used for unit testing by injecting the disk manager
    public boolean init(CountDownLatch latch, DiskManager diskManager) throws StorageException {
        // TODO:
        // read last used sequence id
        try {
            this.checksumGenerator = new ChecksumGenerator();
            this.diskManager = diskManager;
            this.metaStore.init();
        } catch (ChecksumGenerator.ChecksumError checksumError) {
            logger.error(checksumError.getMessage(), checksumError);
            throw new StorageException(checksumError.getMessage(), checksumError);
        }
        return true;
    }

    public boolean startSession(IngestionSession session) {
        if (activeSessions.containsKey(session)) {
            logger.warn("Trying to initiate already existing session. Session id: " + session.getSessionId());
            return true;
        }
        try {
            metaStore.startSession(session); // log first
            activeSessions.put(session, new MemTable<>(memTableSize));
            return true;
        } catch (IOException | StorageException e) {
           logger.error("Error initializing the session. ", e);
            return false;
        }
    }

    public boolean store(IngestionSession session, StrandStorageKey key, StrandStorageValue value) throws StorageException, IOException {
        // todo: data should be first written to a WAL
        // todo: handle error
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        boolean isMemTableFull = memTable.add(key, value);
        if (isMemTableFull) {
            Metadata<StrandStorageKey> metadata = new Metadata<>();
            toSSTable(session, diskManager, metadata);
            metaStore.addSerializedSSTable(session, metadata);
            memTable.clear();
        }
        return true;
    }

    public boolean endSession(IngestionSession session) throws StorageException, IOException {
        // todo: better exception handling
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        if (memTable.getEntryCount() > 0) {
            Metadata<StrandStorageKey> metadata = new Metadata<>();
            toSSTable(session, diskManager, metadata);
            metaStore.addSerializedSSTable(session, metadata);
        }
        metaStore.endSession(session);
        activeSessions.remove(session);
        return true;
    }

    // we inject a disk manager instance for unit testing purposes
    public void toSSTable(IngestionSession session, DiskManager diskManager, Metadata<StrandStorageKey> metadata) throws StorageException {
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        memTable.setReadOnly();
        // we throw the exception for the time being - verify once the upper layer is implemented
        String dir = diskManager.allocate(memTable.getEstimatedSize());
        String storagePath = getSSTableOutputPath(session, dir);
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

    public String getSSTableOutputPath(IngestionSession session, String path) {
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        return path + File.separator + entityId + "_" + memTable.getFirstKey() + "_" + memTable.getLastKey() + "_" + sequenceId++ + ".sd";
    }
}
