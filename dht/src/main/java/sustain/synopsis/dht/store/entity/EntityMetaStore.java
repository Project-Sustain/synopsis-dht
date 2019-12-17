package sustain.synopsis.dht.store.entity;

import sustain.synopsis.dht.store.IngestionSession;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.util.ArrayList;
import java.util.List;

public class EntityMetaStore {
    private final List<IngestionSession> sessions = new ArrayList<>();

    public void startSession(IngestionSession session){

    }

    public void endSession(IngestionSession session){

    }

    public void addSerializedSSTable(IngestionSession session, Metadata<StrandStorageKey> metadata){

    }
}
