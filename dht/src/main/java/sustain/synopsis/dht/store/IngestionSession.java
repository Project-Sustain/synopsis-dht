package sustain.synopsis.dht.store;

import sustain.synopsis.storage.lsmtree.Metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class IngestionSession implements Comparable<IngestionSession> {
    private String ingestionUser;
    private long ingestionTime;
    private long sessionId;
    private boolean complete;
    private List<Metadata<StrandStorageKey>> serializedSSTables;

    public IngestionSession(String ingestionUser, long ingestionTime, long sessionId) {
        this.ingestionUser = ingestionUser;
        this.ingestionTime = ingestionTime;
        this.sessionId = sessionId;
        this.complete = false;
        this.serializedSSTables = new ArrayList<>();
    }

    public void setComplete(){
        this.complete = true;
    }

    public void addSerializedSSTable(Metadata<StrandStorageKey> metadata){
        this.serializedSSTables.add(metadata);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IngestionSession that = (IngestionSession) o;
        return sessionId == that.sessionId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId);
    }

    @Override
    public int compareTo(IngestionSession o) {
        return Long.compare(this.sessionId, o.sessionId);
    }
}
