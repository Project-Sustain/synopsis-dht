package sustain.synopsis.dht.store.entity;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.store.query.Interval;
import sustain.synopsis.dht.store.query.MatchedSSTable;
import sustain.synopsis.dht.store.query.QueryException;
import sustain.synopsis.dht.store.query.QueryUtil;
import sustain.synopsis.dht.store.services.Expression;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Concurrency model: Main thread of a node will run the initialization code. There is a single writer thread and
 * multiple reader threads.
 */
public class EntityStore {
    public final long blockSize;
    private final Logger logger = Logger.getLogger(EntityStore.class);
    private final String entityId;
    /**
     * There can be multiple active ingestion sessions at a given time for a single entity.
     */
    Map<IngestionSession, MemTable<StrandStorageKey, StrandStorageValue>> activeSessions;
    Map<IngestionSession, List<Metadata<StrandStorageKey>>> activeMetadata;
    AtomicInteger sequenceId;
    /**
     * Queryiable metadata is accessed by both the reader threads and the writer threads
     */
    TreeMap<StrandStorageKey, Metadata<StrandStorageKey>> queryableMetadata;
    private DiskManager diskManager;
    // todo: these should be read from the config
    private BlockCompressor compressor;
    private ChecksumGenerator checksumGenerator;
    private EntityStoreJournal entityStoreJournal;
    private long memTableSize;
    private ReentrantReadWriteLock lock;

    public EntityStore(String entityId, String metadataDir, long memTableSize, long blockSize,
                       DiskManager diskManager) {
        this(entityId, new EntityStoreJournal(entityId, metadataDir), memTableSize, blockSize, diskManager);
    }

    // used for unit testing by injecting entity store journal
    public EntityStore(String entityId, EntityStoreJournal entityStoreJournal, long memTableSize, long blockSize,
                       DiskManager diskManager) {
        this.entityId = entityId;
        this.blockSize = blockSize;
        this.activeSessions = new HashMap<>();
        this.activeMetadata = new HashMap<>();
        this.compressor = new LZ4BlockCompressor();
        this.memTableSize = memTableSize;
        this.entityStoreJournal = entityStoreJournal;
        this.queryableMetadata = new TreeMap<>();
        this.sequenceId = new AtomicInteger(-1);
        this.lock = new ReentrantReadWriteLock();
        this.diskManager = diskManager;
    }

    // used for unit testing by injecting the disk manager
    public boolean init() throws StorageException {
        try {
            this.checksumGenerator = new ChecksumGenerator();
            boolean success = this.entityStoreJournal.init();
            if (!success) {
                return false;
            }
            List<Metadata<StrandStorageKey>> metadataList = entityStoreJournal.getMetadata();
            this.sequenceId.set(entityStoreJournal.getSequenceId());
            this.queryableMetadata = metadataList.stream().filter(Metadata::isSessionComplete).collect(Collectors
                                                                                                               .toMap((metadata) -> new StrandStorageKey(
                                                                                                                              metadata.getMin()
                                                                                                                                      .getStartTS(),
                                                                                                                              metadata.getMax()
                                                                                                                                      .getEndTS()),
                                                                                                                      Function.identity(),
                                                                                                                      (o1, o2) -> o1,
                                                                                                                      TreeMap::new));
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

    private void purgeMemTable(IngestionSession session, MemTable<StrandStorageKey, StrandStorageValue> memTable)
            throws IOException, StorageException {
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        metadata.setSessionId(session.getSessionId());
        metadata.setUser(session.getIngestionUser());
        metadata.setSessionStartTS(session.getSessionStartTS());
        // we need to store the memTable session before writing to the commit log
        toSSTable(session, diskManager, metadata);
        entityStoreJournal.addSerializedSSTable(session, metadata);
        activeMetadata.get(session).add(metadata);
        memTable.clear();
    }

    public boolean endSession(IngestionSession session) throws StorageException, IOException {
        if (!activeSessions.containsKey(session)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Unable to end the session. Invalid session id: " + session.getSessionId());
            }
            return false;
        }
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        if (memTable.getEntryCount() > 0) {
            purgeMemTable(session, memTable);
            if (logger.isDebugEnabled()) {
                logger.debug("Ending session: " + session.getSessionId() + ", purged mem table.");
            }
        }
        entityStoreJournal.endSession(session);
        if (logger.isDebugEnabled()) {
            logger.debug("Ending session: " + session.getSessionId() + ", updated commit log.");
        }
        // there can be multiple concurrent reader threads accessing the queryiable data
        try {
            lock.writeLock().lock();
            int beforeSize = queryableMetadata.size();
            queryableMetadata.putAll(activeMetadata.get(session).stream().collect(Collectors
                                                                                          .toMap((metadata) -> new StrandStorageKey(
                                                                                                         metadata.getMin()
                                                                                                                 .getStartTS(),
                                                                                                         metadata.getMax()
                                                                                                                 .getEndTS()),
                                                                                                 Function.identity())));
            int afterSize = queryableMetadata.size();
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Added the SSTables into queryable metadata. Before size: " + beforeSize + ", after " + "size: "
                        + afterSize);
            }
        } finally {
            lock.writeLock().unlock();
        }
        activeSessions.remove(session);
        activeMetadata.remove(session);
        return true;
    }

    // we inject a disk manager instance for unit testing purposes
    public void toSSTable(IngestionSession session, DiskManager diskManager, Metadata<StrandStorageKey> metadata)
            throws StorageException, IOException {
        MemTable<StrandStorageKey, StrandStorageValue> memTable = activeSessions.get(session);
        String dir = diskManager.allocate(memTable.getEstimatedSize());
        String storagePath = getSSTableOutputPath(memTable.getFirstKey(), memTable.getLastKey(), dir, sequenceId.get());
        try (FileOutputStream fos = new FileOutputStream(storagePath);
             DataOutputStream dos = new DataOutputStream(fos)) {
            SSTableWriter<StrandStorageKey, StrandStorageValue> ssTableWriter =
                    new SSTableWriter<>(blockSize, Collections.singletonList(memTable.getIterator()));
            ssTableWriter.serialize(dos, metadata, compressor, checksumGenerator);
            dos.flush();
            fos.flush();
            metadata.setPath(storagePath);
            entityStoreJournal.incrementSequenceId(sequenceId.incrementAndGet());
        } catch (IOException e) {
            logger.error("Error converting memTable to SSTable.", e);
            throw new StorageException("Error converting memTable to SSTable.", e);
        }
    }

    public String getSSTableOutputPath(StrandStorageKey firstKey, StrandStorageKey lastKey, String path, int seqId)
            throws IOException, StorageException {
        return path + File.separator + entityId + "_" + firstKey + "_" + lastKey + "_" + seqId + ".sd";
    }

    public String getEntityId() {
        return entityId;
    }

    /**
     * Query the entity data for a given temporal expression
     *
     * @param temporalExpression Temporal constraint expressed as {@link Expression}
     * @return List of matching SSTables and the corresponding time intervals - A given temporal expression can get
     * mapped into multiple time intervals
     * @throws QueryException Error during temporal expression evaluation
     */
    public List<MatchedSSTable> temporalQuery(Expression temporalExpression) throws QueryException {
        try {
            lock.readLock().lock();
            if (queryableMetadata.isEmpty()) { // there are no completed SSTables yet
                if (logger.isDebugEnabled()) {
                    logger.debug("There are queryable SSTables for the entity: " + entityId);
                }
                return new ArrayList<>();
            }
            Map<String, MatchedSSTable> matchingMetadata = new HashMap<>();
            List<Interval> matchingIntervals = QueryUtil.evaluateTemporalExpression(temporalExpression, new Interval(
                    queryableMetadata.firstKey().getStartTS(), queryableMetadata.lastKey().getEndTS()));
            if (logger.isDebugEnabled()) {
                logger.debug("Number of matching intervals: " + matchingIntervals.size());
            }
            for (Interval interval : matchingIntervals) {
                QueryUtil.temporalLookup(queryableMetadata, interval.getFrom(), interval.getTo(), false).values()
                         .forEach((metadata) -> {
                             matchingMetadata.putIfAbsent(metadata.getPath(), new MatchedSSTable(metadata));
                             MatchedSSTable matchedSSTable = matchingMetadata.get(metadata.getPath());
                             matchedSSTable.addMatchedInterval(interval);
                         });
            }
            return new ArrayList<>(matchingMetadata.values());
        } finally {
            lock.readLock().unlock();
        }
    }
}
