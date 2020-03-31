package sustain.synopsis.dht.store.entity;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.journal.Activity;
import sustain.synopsis.dht.store.IngestionSession;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.journal.JournalLogFactory;
import sustain.synopsis.dht.store.entity.journal.JournalingException;
import sustain.synopsis.dht.store.entity.journal.activity.EndSessionActivity;
import sustain.synopsis.dht.store.entity.journal.activity.IncSeqIdActivity;
import sustain.synopsis.dht.store.entity.journal.activity.SerializeSSTableActivity;
import sustain.synopsis.dht.store.entity.journal.activity.StartSessionActivity;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class EntityStoreJournal {
    private final String metadataRoot;
    private final String entityId;
    private Logger logger = Logger.getLogger(EntityStoreJournal.class);
    private sustain.synopsis.dht.journal.Logger journal;
    private Map<Long, List<Metadata<StrandStorageKey>>> metadataMap = new HashMap<>();
    private int sequenceId = 0;

    public EntityStoreJournal(String entityId, String metaStoreDir) {
        this.metadataRoot = metaStoreDir;
        this.entityId = entityId;
    }

    public boolean init() {
        File metadataFile = new File(getJournalFilePath());
        if (!metadataFile.exists() || (metadataFile.isFile() && metadataFile.length() == 0)) { // empty store
            if (logger.isDebugEnabled()) {
                logger.debug("Instantiating a logger: " + entityId + ", metadata dir: " + metadataFile);
            }
        } else { // existing log
            logger.debug("Using an existing logger. Path: " + metadataFile.getAbsolutePath());
        }
        journal = new sustain.synopsis.dht.journal.Logger(metadataFile.getAbsolutePath());
        Iterator<byte[]> iterator = journal.iterator();
        try {
            parseJournal(iterator);
        } catch (IOException | JournalingException e) {
            return false;
        }
        return true;
    }

    void parseJournal(Iterator<byte[]> iterator) throws IOException, JournalingException {
        // counters used for debugging
        int completeSessionCount = 0;
        int serializedSSTableCount = 0;

        while (iterator.hasNext()) {
            byte[] serialized = iterator.next();
            if (serialized == null) {
                continue;
            }
            Activity activity = JournalLogFactory.parse(serialized);
            switch (activity.getType()) {
                case StartSessionActivity.TYPE:
                    StartSessionActivity startSessionActivity = (StartSessionActivity) activity;
                    metadataMap.put(startSessionActivity.getSessionId(), new ArrayList<>());
                    if (logger.isDebugEnabled()) {
                        logger.debug("Parsed Start Session Activity. " + startSessionActivity);
                    }
                    break;
                case SerializeSSTableActivity.TYPE:
                    SerializeSSTableActivity serializeSSTableActivity = (SerializeSSTableActivity) activity;
                    boolean valid = validateSerializeSSTableActivity(metadataMap, serializeSSTableActivity);
                    if (valid) {
                        metadataMap.get(serializeSSTableActivity.getSessionId())
                                   .add(serializeSSTableActivity.getMetadata());
                        serializedSSTableCount++;
                        if (logger.isDebugEnabled()) {
                            logger.debug("Parsed Serialize SSTable Activity. " + serializeSSTableActivity);
                        }
                    } else {
                        logger.error("Invalid Serialize SStable Activity.");
                        throw new JournalingException(
                                "Invalid Serialize SStable Activity. Aborting parsing the " + "journal");
                    }
                    break;
                case EndSessionActivity.TYPE:
                    EndSessionActivity endSessionActivity = (EndSessionActivity) activity;
                    valid = validateEndSessionActivity(metadataMap, endSessionActivity);
                    if (valid) {
                        completeSessionCount++;
                        metadataMap.get(endSessionActivity.getSessionId())
                                   .forEach(metadata -> metadata.setSessionComplete(true));
                        if (logger.isDebugEnabled()) {
                            logger.debug("Parsed End Session Activity. " + endSessionActivity);
                        }
                    } else {
                        logger.error("Invalid End Session activity.");
                        throw new JournalingException(
                                "Invalid End Session Activity. Aborting parsing the " + "journal");
                    }
                    break;
                case IncSeqIdActivity.TYPE:
                    IncSeqIdActivity incrementSeqIdActivity = (IncSeqIdActivity) activity;
                    sequenceId = incrementSeqIdActivity.getSequenceId();
                    if (logger.isDebugEnabled()) {
                        logger.debug("Sequence Id increment record is detected. " + incrementSeqIdActivity);
                    }
                    break;
            }
        }
        logger.info("Successfully parsed the journal. Total session count: " + metadataMap.size() + ", completed "
                    + "session" + " count: " + completeSessionCount + ", Valid serialized SSTable count: "
                    + serializedSSTableCount + ", last used sequence id: " + sequenceId);
    }

    boolean validateSerializeSSTableActivity(Map<Long, List<Metadata<StrandStorageKey>>> sessions,
                                             SerializeSSTableActivity activity) {
        if (!sessions.containsKey(activity.getSessionId())) {
            logger.error("Missing Start Session Activity. Session Id: " + activity.getSessionId());
            return false; // valid session
        }
        Metadata<StrandStorageKey> metadata = activity.getMetadata();
        File ssTableLoc = new File(metadata.getPath());
        if (!ssTableLoc.exists()) { // ssTable is written
            logger.error("SSTable serialization path is invalid. Path: " + ssTableLoc.getAbsolutePath());
            return false;
        }
        if (!ssTableLoc.isFile()) { // ssTable is a file
            logger.error("SSTable serialization path is not a file. Path: " + ssTableLoc.getAbsolutePath());
            return false;
        }
        return true;
    }

    boolean validateEndSessionActivity(Map<Long, List<Metadata<StrandStorageKey>>> sessionMap,
                                       EndSessionActivity activity) {
        if (!sessionMap.containsKey(activity.getSessionId())) {
            logger.error("Missing Start Session Activity. Session IdL: " + activity.getSessionId());
            return false;
        }
        return true;
    }

    public void startSession(IngestionSession session) throws IOException, StorageException {
        StartSessionActivity startSessionActivityEvent = new StartSessionActivity(session.getSessionId());
        journal.append(startSessionActivityEvent.serialize());
    }

    public void endSession(IngestionSession session) throws IOException, StorageException {
        EndSessionActivity endSessionActivity = new EndSessionActivity(session.getSessionId());
        journal.append(endSessionActivity.serialize());
    }

    public void addSerializedSSTable(IngestionSession session, Metadata<StrandStorageKey> metadata)
            throws IOException, StorageException {
        SerializeSSTableActivity serializeSSTableActivity =
                new SerializeSSTableActivity(session.getSessionId(), metadata);
        journal.append(serializeSSTableActivity.serialize());
    }

    public void incrementSequenceId(int newSequenceId) throws IOException, StorageException {
        journal.append(new IncSeqIdActivity(newSequenceId).serialize());
    }

    String getJournalFilePath() {
        return metadataRoot + File.separator + entityId + "_metadata.slog";
    }

    public List<Metadata<StrandStorageKey>> getMetadata() {
        return metadataMap.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public int getSequenceId() {
        return sequenceId;
    }
}
