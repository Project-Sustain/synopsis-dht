package sustain.synopsis.dht.store.entity;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.IngestionSession;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.journal.activity.EndSessionActivity;
import sustain.synopsis.dht.store.entity.journal.activity.SerializeSSTableActivity;
import sustain.synopsis.dht.store.entity.journal.activity.StartSessionActivity;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.io.IOException;

public class EntityMetaStore {
    private Logger logger = Logger.getLogger(EntityMetaStore.class);
    private final String metadataRoot;
    private final String entityId;
    private sustain.synopsis.dht.journal.Logger journal;

    public EntityMetaStore(String entityId, String metaStoreDir) {
        this.metadataRoot = metaStoreDir;
        this.entityId = entityId;
    }

    public void init() {
        File metadataFile = new File(getMetadataFileName());
        if (!metadataFile.exists() || (metadataFile.isFile() && metadataFile.length() == 0)) { // empty store
            if (logger.isDebugEnabled()) {
                logger.debug("Instantiating a logger: " + entityId + ", metadata dir: " + metadataFile);
            }
        } else { // existing log
            logger.debug("Using an existing logger. Path: " + metadataFile.getAbsolutePath());
        }
        this.journal = new sustain.synopsis.dht.journal.Logger(metadataFile.getAbsolutePath());
        // todo: parse the existing records and
    }

    public void startSession(IngestionSession session) throws IOException, StorageException {
        StartSessionActivity startSessionActivityEvent = new StartSessionActivity(session.getIngestionUser(),
                session.getIngestionTime(), session.getSessionId());
        journal.append(startSessionActivityEvent.serialize());
    }

    public void endSession(IngestionSession session) throws IOException, StorageException {
        EndSessionActivity endSessionActivity = new EndSessionActivity(session.getSessionId());
        journal.append(endSessionActivity.serialize());
    }

    public void addSerializedSSTable(IngestionSession session, Metadata<StrandStorageKey> metadata) throws IOException, StorageException {
        SerializeSSTableActivity serializeSSTableActivity = new SerializeSSTableActivity(session.getSessionId(),
                metadata);
        journal.append(serializeSSTableActivity.serialize());
    }

    private String getMetadataFileName() {
        return metadataRoot + File.separator + entityId + "_metadata.slog";
    }

}
