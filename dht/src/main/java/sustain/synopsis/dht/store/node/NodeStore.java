package sustain.synopsis.dht.store.node;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.Util;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.query.QueryException;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.workers.WriterPool;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Orchestrates data storage within a storage node. Keeps track of various entity stores, organized by the dataset.
 * Creation of entity stores is recorded in a commit log to withstand node crashes and restarts.
 * <p>
 * Concurrency model: Writes to each entity store is handled by a single writer. However, there can be multiple
 * concurrent write requests (using the #store()) at a NodeStore. There can be multiple reader threads for a given
 * entity store. The node initialization is handled by the main thread during the startup of the node.
 */
public class NodeStore {
    // package level access to support unit testing
    Map<String, Trie<String, EntityStore>> entityStoreMap = new ConcurrentHashMap<>();
    Map<Long, SessionValidator.SessionValidationResponse> validatedSessions = new ConcurrentHashMap<>();
    private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(NodeStore.class);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private SessionValidator sessionValidator;
    private AtomicBoolean initialized;
    private Logger rootLogger;
    private int memTableSize;
    private int blockSize;
    private String metadataStoreDir;
    private DiskManager diskManager;


    public NodeStore() {
        this(new SessionValidator(),
             new Logger(getRootJournalFileName(Context.getInstance().getNodeConfig().getRootJournalLoc())),
             Context.getInstance().getNodeConfig().getMemTableSize() * 1024 * 1024, // MB -> Bytes
             Context.getInstance().getNodeConfig().getBlockSize() * 1024 * 1024, // MB -> Bytes
             Context.getInstance().getNodeConfig().getMetadataStoreDir(), new DiskManager());
    }

    // constructor used for writing unit tests with mocks
    public NodeStore(SessionValidator validator, Logger logger, int memTableSize, int blockSize,
                     String metadataStoreDir, DiskManager diskManager) {
        this.sessionValidator = validator;
        this.memTableSize = memTableSize;
        this.blockSize = blockSize;
        this.rootLogger = logger;
        this.metadataStoreDir = metadataStoreDir;
        this.initialized = new AtomicBoolean(false);
        this.diskManager = diskManager;
    }

    static String getRootJournalFileName(String rootJournalLoc) {
        return rootJournalLoc + File.separator + Util.getHostname() + "_root.slog";
    }

    /**
     * Initialize the node store based on the commit log. Reads log records of the all entity stores created for
     * different data sets: initialize them and keep an index of them in an in-memory structure for subsequent data
     * ingestion/querying purposes.
     *
     * @throws StorageException Error when parsing the commit log
     */
    public synchronized void init() throws StorageException {
        if (initialized.get()) {
            return;
        }
        logger.info("Starting node store initialization.");
        long nodeInitStartTS = System.currentTimeMillis();

        // initialize the disk manager
        boolean diskManagerReady = diskManager.init(Context.getInstance().getNodeConfig());
        if (!diskManagerReady) {
            throw new StorageException("Failed to initialize disk manager.");
        }

        // initialize all entity stores
        for (byte[] serialized : rootLogger) {
            if (serialized == null) {
                continue;
            }
            processCommitLogEntry(serialized);
        }
        initialized = new AtomicBoolean(true);
        long nodeInitEndTS = System.currentTimeMillis();
        logger.info("Completed node store initialization. Elapsed time (ms): " + (nodeInitEndTS - nodeInitStartTS));
    }

    private void processCommitLogEntry(byte[] serialized) throws StorageException {
        try {
            CreateEntityStoreActivity createEntityStore = new CreateEntityStoreActivity();
            createEntityStore.deserialize(serialized);
            EntityStore entityStore =
                    new EntityStore(createEntityStore.getDataSetId(), createEntityStore.getEntityId(), metadataStoreDir,
                                    memTableSize, blockSize, diskManager);
            if (logger.isDebugEnabled()) {
                logger.debug("Initializing entity store: " + createEntityStore.getEntityId());
            }
            long entityStoreStartTS = System.currentTimeMillis();
            entityStore.init();
            long entityStoreEndTS = System.currentTimeMillis();
            if (logger.isDebugEnabled()) {
                logger.debug("Completed initializing entity store: " + createEntityStore.getEntityId() + ", "
                             + "elapsed time (s): " + (entityStoreEndTS - entityStoreStartTS) / (1000.0));
            }
            entityStoreMap.putIfAbsent(createEntityStore.getDataSetId(), new PatriciaTrie<>());
            entityStoreMap.get(createEntityStore.getDataSetId()).put(createEntityStore.getEntityId(), entityStore);
        } catch (IOException | StorageException e) {
            logger.error(e);
            throw new StorageException("Error initializing node store.", e);
        }
    }

    /**
     * Store the given key and value in the appropriate entity store. Session is validated before performing
     * the storage operation.
     * @param datasetId Dataset identifier
     * @param entityId Entity identifier
     * @param sessionId Session Id
     * @param storageKey Key for the storage object
     * @param storageValue Value for the storage object
     * @throws IOException Error when storing strands on disk
     * @throws StorageException Error during serialization
     */
    public void store(String datasetId, String entityId, long sessionId, StrandStorageKey storageKey,
                      StrandStorageValue storageValue) throws IOException, StorageException {
        // check if the session is new. If it's a new session validate.
        // It's not necessary to record this in the commit log. A session can revalidated and cached in case
        // of node failure/restart.
        // It is possible that there will be multiple concurrent request to validate the same session in this code
        // which is acceptable and will not affect the accuracy of the program.
        SessionValidator.SessionValidationResponse response = validateSession(datasetId, sessionId);
        if (!response.valid) {
            logger.error("Session validation failed. Aborting ingestion operation. User id: " + response.userId + ", "
                         + "data set id: " + datasetId + ", session id: " + sessionId);
            throw new StorageException("Session validation failed. Session id: " + sessionId);
        }
        store(datasetId, entityId, new IngestionSession(response.userId, response.sessionStartTS, sessionId),
              storageKey, storageValue);
    }

    private SessionValidator.SessionValidationResponse validateSession(String datasetId, long sessionId) {
        if (validatedSessions.containsKey(sessionId)) {
            return validatedSessions.get(sessionId);
        }
        // it is okay to perform duplicate lookups (which may happens a few times per session) - otherwise we will have
        // to synchronize
        SessionValidator.SessionValidationResponse response = sessionValidator.validate(datasetId, sessionId);
        if (!validatedSessions.containsKey(sessionId)) {
            validatedSessions.put(sessionId, response); // it is okay if multiple threads overwrite the same value
        }
        return response;
    }
    
    void store(String datasetId, String entityId, IngestionSession session, StrandStorageKey storageKey,
               StrandStorageValue storageValue) throws StorageException, IOException {
        if (!entityStoreMap.containsKey(datasetId)) { // check if the dataset is new
            try {
                lock.writeLock().lock();
                entityStoreMap.putIfAbsent(datasetId, new PatriciaTrie<>());
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding a new dataset. Dataset Id: " + datasetId);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        if (!entityStoreMap.get(datasetId).containsKey(entityId)) { // check if entity store is new
            try {
                lock.writeLock().lock();
                if (!entityStoreMap.get(datasetId).containsKey(entityId)) {
                    createEntityStore(datasetId, entityId);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        EntityStore entityStore = entityStoreMap.get(datasetId).get(entityId);
        entityStore.store(session, storageKey, storageValue);
    }

    private void createEntityStore(String datasetId, String entityId) throws StorageException, IOException {
        EntityStore store =
                new EntityStore(datasetId, entityId, metadataStoreDir, memTableSize, blockSize, diskManager);
        store.init();
        // If a new entity store is created, journal its metadata location
        CreateEntityStoreActivity createEntityStoreActivity = new CreateEntityStoreActivity(datasetId, entityId);
        rootLogger.append(createEntityStoreActivity.serialize());
        entityStoreMap.get(datasetId).put(entityId, store);
        if (logger.isDebugEnabled()) {
            logger.debug("Added and initialized a new entity store. Dataset id: " + datasetId + ", " + "entity id: "
                         + entityId + ", total entity count: " + entityStoreMap.get(datasetId).size());
        }
    }

    /**
     * End a session for all entity stores under purview of the current node store
     *
     * @param datasetId Dataset identifier. If there are data with this dataset id, a completed future with
     *                  <code>true</code> is returned - This can happen in a multi node setup.
     * @param sessionId Session id. The session id is validated before ending the session. If the session validation
     *                  fails, then a a completed future with <code>false</code> is returned.
     * @param writers   Set of writer threads used for ingesting data. Because entity stores assumes single writers, it
     *                  is important that the same thread which ingested data ends the session (there can be some data
     *                  remaining in the memTable).
     * @return list of {@link CompletableFuture<Boolean>} with a future for each entity store
     */
    public List<CompletableFuture<Boolean>> endSession(String datasetId, long sessionId, WriterPool writers) {
        // check if there is a dataset with the given id - some nodes may not have received data for the given dataset
        if (!entityStoreMap.containsKey(datasetId)) {
            CompletableFuture<Boolean> returnValue = new CompletableFuture<>();
            returnValue.complete(true);
            return Collections.singletonList(returnValue);
        }
        // validate session
        SessionValidator.SessionValidationResponse response = validateSession(datasetId, sessionId);
        if (!response.valid) {
            CompletableFuture<Boolean> returnValue = new CompletableFuture<>();
            returnValue.complete(false);
            return Collections.singletonList(returnValue);
        }
        return entityStoreMap.get(datasetId).keySet().stream().map(entityId -> CompletableFuture.supplyAsync(
                () -> endEntityStoreSession(datasetId, entityId, response.userId, sessionId, response.sessionStartTS),
                writers.getExecutor(entityId.hashCode()))).collect(Collectors.toList());
    }

    /**
     * Terminate an ingestion session for a given entity.
     *
     * @param datasetId      Dataset id
     * @param entityId       Entity id
     * @param user           User id
     * @param sessionId      Session id
     * @param sessionStartTS Session start time
     * @return <code>true</code> if session termination was successful, <code>false</code> otherwise
     */
    public boolean endEntityStoreSession(String datasetId, String entityId, String user, long sessionId,
                                         long sessionStartTS) {
        try {
            // we use the IngestionSession constructor with the session id here. It is used only for
            // looking up a session.
            return entityStoreMap.get(datasetId).get(entityId)
                                 .endSession(new IngestionSession(user, sessionStartTS, sessionId));
        } catch (StorageException | IOException e) {
            logger.error(
                    "Error terminating session on the entity store. Dataset Id: " + datasetId + ", session " + "id:"
                    + sessionId + ", entity id: " + entityId);
            return false;
        }
    }

    /**
     * Get the list of matching entity stores for a given query.
     *
     * @param queryRequest Query request
     * @return List of matching entity stores
     */
    public Set<EntityStore> getMatchingEntityStores(TargetQueryRequest queryRequest) throws QueryException {
        try {
            lock.readLock().lock();
            String dataset = queryRequest.getDataset();
            if (!entityStoreMap.containsKey(dataset)) {
                throw new QueryException("Non existing dataset: " + dataset);
            }
            Trie<String, EntityStore> entityStores = entityStoreMap.get(dataset);
            List<Predicate> spatialPredicates = queryRequest.getSpatialScopeList();
            // Currently we expect the spatial scopes to be available as geohash prefix
            return spatialPredicates.stream().map(Predicate::getStringValue).map(entityStores::prefixMap).
                    flatMap(sortedMap -> sortedMap.values().stream()).collect(Collectors.toSet());
        } finally {
            lock.readLock().unlock();
        }
    }
}
