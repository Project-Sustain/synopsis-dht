package sustain.synopsis.dht.store.entity.journal.activity;

import sustain.synopsis.dht.journal.AbstractActivity;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EndSessionActivity extends AbstractActivity {

    private final short TYPE = 2;
    private long sessionId;

    public EndSessionActivity() {
        // for deserialization
    }

    public EndSessionActivity(long sessionId) {
        this.sessionId = sessionId;
    }

    @Override
    public void serializeMembers(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeLong(sessionId);
    }

    @Override
    public void deserializeMembers(DataInputStream dataInputStream) throws IOException {
        this.sessionId = dataInputStream.readLong();
    }

    @Override
    public short getType() {
        return TYPE;
    }

    public long getSessionId() {
        return sessionId;
    }
}
