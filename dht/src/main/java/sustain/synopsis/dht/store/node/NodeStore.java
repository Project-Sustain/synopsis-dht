/*
 *
 * Software in the Sustain Ecosystem are Released Under Terms of Apache Software License 
 *
 * This research has been supported by funding from the US National Science Foundation's CSSI program through awards 1931363, 1931324, 1931335, and 1931283. The project is a joint effort involving Colorado State University, Arizona State University, the University of California-Irvine, and the University of Maryland - Baltimore County. All redistributions of the software must also include this information. 
 *
 * TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
 *
 *
 * 1. Definitions.
 *
 * "License" shall mean the terms and conditions for use, reproduction, and distribution as defined by Sections 1 through 9 of this document.
 *
 * "Licensor" shall mean the copyright owner or entity authorized by the copyright owner that is granting the License.
 *
 * "Legal Entity" shall mean the union of the acting entity and all other entities that control, are controlled by, or are under common control with that entity. For the purposes of this definition, "control" means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.
 *
 * "You" (or "Your") shall mean an individual or Legal Entity exercising permissions granted by this License.
 *
 * "Source" form shall mean the preferred form for making modifications, including but not limited to software source code, documentation source, and configuration files.
 *
 * "Object" form shall mean any form resulting from mechanical transformation or translation of a Source form, including but not limited to compiled object code, generated documentation, and conversions to other media types.
 *
 * "Work" shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work (an example is provided in the Appendix below).
 *
 * "Derivative Works" shall mean any work, whether in Source or Object form, that is based on (or derived from) the Work and for which the editorial revisions, annotations, elaborations, or other modifications represent, as a whole, an original work of authorship. For the purposes of this License, Derivative Works shall not include works that remain separable from, or merely link (or bind by name) to the interfaces of, the Work and Derivative Works thereof.
 *
 * "Contribution" shall mean any work of authorship, including the original version of the Work and any modifications or additions to that Work or Derivative Works thereof, that is intentionally submitted to Licensor for inclusion in the Work by the copyright owner or by an individual or Legal Entity authorized to submit on behalf of the copyright owner. For the purposes of this definition, "submitted" means any form of electronic, verbal, or written communication sent to the Licensor or its representatives, including but not limited to communication on electronic mailing lists, source code control systems, and issue tracking systems that are managed by, or on behalf of, the Licensor for the purpose of discussing and improving the Work, but excluding communication that is conspicuously marked or otherwise designated in writing by the copyright owner as "Not a Contribution."
 *
 * "Contributor" shall mean Licensor and any individual or Legal Entity on behalf of whom a Contribution has been received by Licensor and subsequently incorporated within the Work.
 *
 * 2. Grant of Copyright License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare Derivative Works of, publicly display, publicly perform, sublicense, and distribute the Work and such Derivative Works in Source or Object form.
 *
 * 3. Grant of Patent License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such license applies only to those patent claims licensable by such Contributor that are necessarily infringed by their Contribution(s) alone or by combination of their Contribution(s) with the Work to which such Contribution(s) was submitted. If You institute patent litigation against any entity (including a cross-claim or counterclaim in a lawsuit) alleging that the Work or a Contribution incorporated within the Work constitutes direct or contributory patent infringement, then any patent licenses granted to You under this License for that Work shall terminate as of the date such litigation is filed.
 *
 * 4. Redistribution. You may reproduce and distribute copies of the Work or Derivative Works thereof in any medium, with or without modifications, and in Source or Object form, provided that You meet the following conditions:
 *
 * You must give any other recipients of the Work or Derivative Works a copy of this License; and
 * You must cause any modified files to carry prominent notices stating that You changed the files; and
 * You must retain, in the Source form of any Derivative Works that You distribute, all copyright, patent, trademark, and attribution notices from the Source form of the Work, excluding those notices that do not pertain to any part of the Derivative Works; and
 * If the Work includes a "NOTICE" text file as part of its distribution, then any Derivative Works that You distribute must include a readable copy of the attribution notices contained within such NOTICE file, excluding those notices that do not pertain to any part of the Derivative Works, in at least one of the following places: within a NOTICE text file distributed as part of the Derivative Works; within the Source form or documentation, if provided along with the Derivative Works; or, within a display generated by the Derivative Works, if and wherever such third-party notices normally appear. The contents of the NOTICE file are for informational purposes only and do not modify the License. You may add Your own attribution notices within Derivative Works that You distribute, alongside or as an addendum to the NOTICE text from the Work, provided that such additional attribution notices cannot be construed as modifying the License. 
 *
 * You may add Your own copyright statement to Your modifications and may provide additional or different license terms and conditions for use, reproduction, or distribution of Your modifications, or for any such Derivative Works as a whole, provided Your use, reproduction, and distribution of the Work otherwise complies with the conditions stated in this License.
 * 5. Submission of Contributions. Unless You explicitly state otherwise, any Contribution intentionally submitted for inclusion in the Work by You to the Licensor shall be under the terms and conditions of this License, without any additional terms or conditions. Notwithstanding the above, nothing herein shall supersede or modify the terms of any separate license agreement you may have executed with Licensor regarding such Contributions.
 *
 * 6. Trademarks. This License does not grant permission to use the trade names, trademarks, service marks, or product names of the Licensor, except as required for reasonable and customary use in describing the origin of the Work and reproducing the content of the NOTICE file.
 *
 * 7. Disclaimer of Warranty. Unless required by applicable law or agreed to in writing, Licensor provides the Work (and each Contributor provides its Contributions) on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of permissions under this License.
 *
 * 8. Limitation of Liability. In no event and under no legal theory, whether in tort (including negligence), contract, or otherwise, unless required by applicable law (such as deliberate and grossly negligent acts) or agreed to in writing, shall any Contributor be liable to You for damages, including any direct, indirect, special, incidental, or consequential damages of any character arising as a result of this License or out of the use or inability to use the Work (including but not limited to damages for loss of goodwill, work stoppage, computer failure or malfunction, or any and all other commercial damages or losses), even if such Contributor has been advised of the possibility of such damages.
 *
 * 9. Accepting Warranty or Additional Liability. While redistributing the Work or Derivative Works thereof, You may choose to offer, and charge a fee for, acceptance of support, warranty, indemnity, or other liability obligations and/or rights consistent with this License. However, in accepting such obligations, You may act only on Your own behalf and on Your sole responsibility, not on behalf of any other Contributor, and only if You agree to indemnify, defend, and hold each Contributor harmless for any liability incurred by, or claims asserted against, such Contributor by reason of your accepting any such warranty or additional liability. 
 *
 * END OF TERMS AND CONDITIONS */
package sustain.synopsis.dht.store.node;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.Util;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.services.ingestion.WriterPool;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.dht.store.services.TargetQueryRequest;

import java.io.File;
import java.io.IOException;
import java.util.*;
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
     * Store the given key and value in the appropriate entity store. Session is validated before performing the storage
     * operation.
     *
     * @param datasetId    Dataset identifier
     * @param entityId     Entity identifier
     * @param sessionId    Session Id
     * @param storageKey   Key for the storage object
     * @param storageValue Value for the storage object
     * @throws IOException      Error when storing strands on disk
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
    public Set<EntityStore> getMatchingEntityStores(TargetQueryRequest queryRequest) {
        try {
            lock.readLock().lock();
            String dataset = queryRequest.getDataset();
            if (!entityStoreMap.containsKey(dataset)) {
                return new HashSet<>();
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
