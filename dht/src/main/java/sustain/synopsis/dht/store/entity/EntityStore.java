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
package sustain.synopsis.dht.store.entity;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.services.query.Interval;
import sustain.synopsis.dht.services.query.MatchedSSTable;
import sustain.synopsis.dht.services.query.QueryException;
import sustain.synopsis.dht.services.query.QueryUtil;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.storage.lsmtree.*;
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
    private final String datasetId;
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
    private BlockCompressor compressor;
    private ChecksumGenerator checksumGenerator;
    private EntityStoreJournal entityStoreJournal;
    private long memTableSize;
    private ReentrantReadWriteLock lock;

    public EntityStore(String datasetId, String entityId, String metadataDir, long memTableSize, long blockSize,
                       DiskManager diskManager) {
        this(datasetId, entityId, new EntityStoreJournal(datasetId, entityId, metadataDir), memTableSize, blockSize,
             diskManager);
    }

    // used for unit testing by injecting entity store journal
    public EntityStore(String datasetId, String entityId, EntityStoreJournal entityStoreJournal, long memTableSize,
                       long blockSize, DiskManager diskManager) {
        this.datasetId = datasetId;
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

    /**
     * Store the data in the MemTable first. If the MemTable is full, the purge it to disk.
     * @param session {@link IngestionSession} session corresponding to the data
     * @param key Key
     * @param value Actual value to be stored
     * @throws StorageException Error during serialization or commit log update
     */
    public void store(IngestionSession session, StrandStorageKey key, StrandStorageValue value)
            throws StorageException {
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
        } catch (IOException | StorageException | MergeError e) {
            logger.error("Error storing the strand.", e);
            throw new StorageException(e.getMessage(), e);
        }
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
            return true;
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

    public String getSSTableOutputPath(StrandStorageKey firstKey, StrandStorageKey lastKey, String path, int seqId) {
        return path + File.separator + datasetId + "_" + entityId + "_" + firstKey + "_" + lastKey + "_" + seqId
               + ".sd";
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
