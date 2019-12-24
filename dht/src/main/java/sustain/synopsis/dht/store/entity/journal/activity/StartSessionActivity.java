package sustain.synopsis.dht.store.entity.journal.activity;

import sustain.synopsis.dht.journal.AbstractActivity;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StartSessionActivity extends AbstractActivity {

    public static final byte TYPE = 0;
    private String user;
    private long ingestionTimeStamp;
    private long sessionId;

    public StartSessionActivity() {
    }

    public StartSessionActivity(String user, long ingestionTimeStamp, long sessionId) {
        this.user = user;
        this.ingestionTimeStamp = ingestionTimeStamp;
        this.sessionId = sessionId;
    }

    @Override
    public void setType(short type) {

    }

    @Override
    public void serializeMembers(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(user);
        dataOutputStream.writeLong(ingestionTimeStamp);
        dataOutputStream.writeLong(sessionId);
    }

    @Override
    public void deserializeMembers(DataInputStream dataInputStream) throws IOException {
        this.user = dataInputStream.readUTF();
        this.ingestionTimeStamp = dataInputStream.readLong();
        this.sessionId = dataInputStream.readLong();
    }

    @Override
    public short getType() {
        return TYPE;
    }

    public String getUser() {
        return user;
    }

    public long getIngestionTimeStamp() {
        return ingestionTimeStamp;
    }

    public long getSessionId() {
        return sessionId;
    }
}
