package sustain.synopsis.dht.store.entity.journal.activity;

import sustain.synopsis.dht.journal.AbstractActivity;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class  SerializeSSTableActivity extends AbstractActivity {
    private final short TYPE = 1;
    private long sessionId;
    private Metadata<StrandStorageKey> metadata;

    public SerializeSSTableActivity(){

    }

    public SerializeSSTableActivity(long sessionId, Metadata<StrandStorageKey> metadata){
        this.sessionId = sessionId;
        this.metadata = metadata;
    }

    @Override
    public void serializeMembers(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(sessionId);
        metadata.serialize(dataOutputStream);
    }

    @Override
    public void deserializeMembers(DataInputStream dataInputStream) throws IOException {
        this.sessionId = dataInputStream.readLong();
        this.metadata = new Metadata<>();
        metadata.deserialize(dataInputStream, StrandStorageKey.class);
    }

    @Override
    public short getType() {
        return TYPE;
    }

    public long getSessionId() {
        return sessionId;
    }

    public Metadata<StrandStorageKey> getMetadata() {
        return metadata;
    }
}
