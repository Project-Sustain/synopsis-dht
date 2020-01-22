package sustain.synopsis.dht.store.entity.journal.activity;

import sustain.synopsis.dht.journal.AbstractActivity;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class IncSeqIdActivity extends AbstractActivity {

    public static final short TYPE = 3;

    private int sequenceId;

    public IncSeqIdActivity(){
        // for deserialization
    }

    public IncSeqIdActivity(int sequenceId) {
        this.sequenceId = sequenceId;
    }

    @Override
    public void serializeMembers(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeInt(sequenceId);
    }

    @Override
    public void deserializeMembers(DataInputStream dataInputStream) throws IOException {
        this.sequenceId = dataInputStream.readInt();
    }

    @Override
    public short getType() {
        return TYPE;
    }

    public int getSequenceId() {
        return sequenceId;
    }
}
