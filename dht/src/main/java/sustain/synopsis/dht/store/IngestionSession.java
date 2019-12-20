package sustain.synopsis.dht.store;

import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
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

    public void setComplete() {
        this.complete = true;
    }

    public void addSerializedSSTable(Metadata<StrandStorageKey> metadata) {
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

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeUTF(ingestionUser);
        dos.writeLong(ingestionTime);
        dos.writeLong(sessionId);
        dos.writeBoolean(complete);
        dos.writeInt(serializedSSTables.size());
        for (Metadata<StrandStorageKey> metadata : serializedSSTables) {
            metadata.serialize(dos);
        }
    }

    public static IngestionSession deserialize(DataInputStream dis) throws IOException, InstantiationException,
            IllegalAccessException {
        String ingestionUser = dis.readUTF();
        long ingestionTime = dis.readLong();
        long sessionId = dis.readLong();

        IngestionSession ingestionSession = new IngestionSession(ingestionUser, ingestionTime, sessionId);
        if (dis.readBoolean()) {
            ingestionSession.setComplete();
        }
        int ssTableCount = dis.readInt();
        for (int i = 0; i < ssTableCount; i++) {
            Metadata<StrandStorageKey> metadata = new Metadata<>();
            metadata.deserialize(dis, StrandStorageKey.class);
            ingestionSession.addSerializedSSTable(metadata);
        }
        return ingestionSession;
    }

    public String getIngestionUser() {
        return ingestionUser;
    }

    public long getIngestionTime() {
        return ingestionTime;
    }

    public long getSessionId() {
        return sessionId;
    }

    public boolean isComplete() {
        return complete;
    }

    public List<Metadata<StrandStorageKey>> getSerializedSSTables() {
        return serializedSSTables;
    }
}
