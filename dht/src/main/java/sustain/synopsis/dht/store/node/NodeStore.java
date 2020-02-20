package sustain.synopsis.dht.store.node;

import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.Util;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.store.entity.EntityStore;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Orchestrates data storage within a storage node. Keeps track of various entity stores, organized by the dataset.
 * Creation of entity stores is recorded in a commit log to withstand node crashes and restarts.
 * <p>
 * Concurrency model: Writes to each entity store is handled by a single writer. However, there can be
 * multiple concurrent write requests (using the #store()) at a NodeStore. There can be multiple reader threads
 * for a given entity store. The node initialization is handled by the main thread during the startup of the node.
 */
public class NodeStore {
    // package level access to support unit testing
    Map<String, Map<String, EntityStore>> entityStoreMap = new ConcurrentHashMap<>();
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


    public NodeStore() throws StorageException {
        this(new SessionValidator(),
                new Logger(getRootJournalFileName(Context.getInstance().getNodeConfig().getRootJournalLoc())),
                Context.getInstance().getNodeConfig().getMemTableSize(),
                Context.getInstance().getNodeConfig().getBlockSize(),
                Context.getInstance().getNodeConfig().getMetadataStoreDir(), DiskManager.getInstance());
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

    /**
     * Initialize the node store based on the commit log. Reads log records of the
     * all entity stores created for different data sets: initialize them and keep an index of them
     * in an in-memory structure for subsequent data ingestion/querying purposes.
     *
     * @throws StorageException Error when parsing the commit log
     */
    public synchronized void init() throws StorageException {
        if (initialized.get()) {
            return;
        }
        logger.info("Starting node store initialization.");
        long nodeInitStartTS = System.currentTimeMillis();
        // initialize all entity stores
        Iterator<byte[]> iterator = rootLogger.iterator();
        while (iterator.hasNext()) {
            byte[] serialized = iterator.next();
            if (serialized == null) {
                continue;
            }
            try {
                CreateEntityStoreActivity createEntityStore = new CreateEntityStoreActivity();
                createEntityStore.deserialize(serialized);
                EntityStore entityStore = new EntityStore(createEntityStore.getEntityId(),
                        createEntityStore.getEntityJournalLogLocation(), memTableSize, blockSize, diskManager);
                if (logger.isDebugEnabled()) {
                    logger.debug("Initializing entity store: " + createEntityStore.getEntityId());
                }
                long entityStoreStartTS = System.currentTimeMillis();
                entityStore.init();
                long entityStoreEndTS = System.currentTimeMillis();
                if (logger.isDebugEnabled()) {
                    logger.debug("Completed initializing entity store: " + createEntityStore.getEntityId() + ", " +
                            "elapsed time (s): " + (entityStoreEndTS - entityStoreStartTS) / (1000.0));
                }
                entityStoreMap.putIfAbsent(createEntityStore.getDataSetId(), new ConcurrentHashMap<>());
                entityStoreMap.get(createEntityStore.getDataSetId()).put(createEntityStore.getEntityId(), entityStore);
            } catch (IOException | StorageException e) {
                logger.error(e);
                throw new StorageException("Error initializing node store.", e);
            }
        }
        initialized = new AtomicBoolean(true);
        long nodeInitEndTS = System.currentTimeMillis();
        logger.info("Completed node store initialization. Elapsed time (s): " + (nodeInitEndTS - nodeInitStartTS) / 1000.0);
    }

    public void store(String datasetId, String entityId, long sessionId, StrandStorageKey storageKey,
                      StrandStorageValue storageValue) throws IOException,
            StorageException {
        // check if the session is new. If it's a new session validate.
        // It's not necessary to record this in the commit log. A session can revalidated and cached in case
        // of node failure/restart.
        // It is possible that there will be multiple concurrent request to validate the same session in this code
        // which is acceptable and will not affect the accuracy of the program.
        SessionValidator.SessionValidationResponse response;
        if (!validatedSessions.containsKey(sessionId)) {
            response = sessionValidator.validate(datasetId, sessionId);
            if (!response.valid) {
                logger.error("Session validation failed. Aborting ingestion operation. User id: " + response.userId + ", " + "data set id: " + datasetId + ", session id: " + sessionId);
                return;
            }
            validatedSessions.put(sessionId, response);
        } else {
            response = validatedSessions.get(sessionId);
        }
        store(response.userId, datasetId, entityId, sessionId, response.sessionStartTS, storageKey, storageValue);
    }

    /**
     * Store an individual strand in the corresponding entity store
     *
     * @param userId            Identifier of the user performing the ingestion operation
     * @param datasetId         Dataset identifier
     * @param entityId          Entity identifier (spatial scope)
     * @param sessionId         Current session id
     * @param sessionCreationTs Session creation timestamp returned by the metadata server
     * @param storageKey        Strand storage key
     * @param storageValue      Strand storage value
     * @throws StorageException Error when storing strands on disk
     * @throws IOException      Error during serialization
     */
    void store(String userId, String datasetId, String entityId, long sessionId, long sessionCreationTs,
                      StrandStorageKey storageKey, StrandStorageValue storageValue) throws StorageException,
            IOException {
        // retrieve entity store
        if (!entityStoreMap.containsKey(datasetId)) {
            try {
                lock.writeLock().lock();
                entityStoreMap.putIfAbsent(datasetId, new ConcurrentHashMap<>());
                if (logger.isDebugEnabled()) {
                    logger.debug("Adding a new dataset. Dataset Id: " + datasetId);
                }
            } finally {
                lock.writeLock().unlock();
            }
        }
        EntityStore entityStore;
        if (!entityStoreMap.get(datasetId).containsKey(entityId)) {
            try {
                lock.writeLock().lock();
                // we need to check again if a previous invocation using a different thread have successfully
                // initialized the entity store
                if (!entityStoreMap.get(datasetId).containsKey(entityId)) {
                    entityStore = new EntityStore(entityId, metadataStoreDir, memTableSize, blockSize, diskManager);
                    entityStore.init();
                    // If a new entity store is created, journal its metadata location
                    CreateEntityStoreActivity createEntityStoreActivity = new CreateEntityStoreActivity(datasetId,
                            entityId, entityStore.getJournalFilePath());
                    rootLogger.append(createEntityStoreActivity.serialize());
                    entityStoreMap.get(datasetId).put(entityId, entityStore);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Added and initialized a new entity store. Dataset id: " + datasetId + ", " +
                                "entity id: " + entityId);
                    }
                }
            } catch (StorageException e) {
                logger.error("Error initializing a new entity store.", e);
                throw e;
            } finally {
                lock.writeLock().unlock();
            }
        }
        entityStore = entityStoreMap.get(datasetId).get(entityId);
        // Store the strand - check the status of the storage
        // we create a new instance of IngestionSession because each entity store uses separate IngestionSession
        // objects. Two IngestionSession objects are considered the same if they have the same session id.
        entityStore.store(new IngestionSession(userId, sessionCreationTs, sessionId), storageKey, storageValue);
    }

    /**
     * Terminate session. Informs all entity stores of the corresponding dataset to terminate their sessions.
     * Terminating a session makes the data ingested during that session available for the subsequent queries.
     *
     * @param datasetId      Dataset Id
     * @param user           User Id
     * @param sessionId      Session id
     * @param sessionStartTS session start timestamp
     */
    public void endSession(String datasetId, String user, long sessionId, long sessionStartTS) {
        // Acknowledge every entity store of the dataset
        // There may be sessions that did not receive any data for this session - this case is handled by the entity
        // store.
        entityStoreMap.get(datasetId).forEach((entityId, entityStore) -> {
            try {
                // we use the IngestionSession constructor with the session id here. It is used only for
                // looking up a session.
                entityStore.endSession(new IngestionSession(user, sessionId, sessionStartTS));
            } catch (StorageException | IOException e) {
                logger.error("Error terminating session on the entity store. Dataset Id: " + datasetId + ", session " + "id:" + sessionId + ", entity id: " + entityId);
            }
        });
        if (logger.isDebugEnabled()) {
            logger.debug("Processed end session message. Dataset id: " + datasetId + ", session id: " + sessionId);
        }
    }


    static String getRootJournalFileName(String rootJournalLoc) {
        return rootJournalLoc + File.separator + Util.getHostname() + "_root.slog";
    }
}
