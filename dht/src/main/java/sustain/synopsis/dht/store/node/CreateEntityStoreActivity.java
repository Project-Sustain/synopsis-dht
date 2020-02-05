package sustain.synopsis.dht.store.node;

import sustain.synopsis.dht.journal.AbstractActivity;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class CreateEntityStoreActivity extends AbstractActivity {

    public static final short TYPE = 4;
    private String dataSetId;
    private String entityId;
    private String entityJournalLogLocation;

    public CreateEntityStoreActivity() {
        // for serialization
    }

    public CreateEntityStoreActivity(String dataSetId, String entityId, String entityJournalLogLocation) {
        this.dataSetId = dataSetId;
        this.entityId = entityId;
        this.entityJournalLogLocation = entityJournalLogLocation;
    }

    @Override
    public void serializeMembers(DataOutputStream dataOutputStream) throws IOException {
        dataOutputStream.writeUTF(dataSetId);
        dataOutputStream.writeUTF(entityId);
        dataOutputStream.writeUTF(entityJournalLogLocation);
    }

    @Override
    public void deserializeMembers(DataInputStream dataInputStream) throws IOException {
        this.dataSetId = dataInputStream.readUTF();
        this.entityId = dataInputStream.readUTF();
        this.entityJournalLogLocation = dataInputStream.readUTF();
    }

    @Override
    public short getType() {
        return TYPE;
    }

    public String getDataSetId() {
        return dataSetId;
    }

    public String getEntityId() {
        return entityId;
    }

    public String getEntityJournalLogLocation() {
        return entityJournalLogLocation;
    }
}
