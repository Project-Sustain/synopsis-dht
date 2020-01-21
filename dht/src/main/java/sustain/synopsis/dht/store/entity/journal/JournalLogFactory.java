package sustain.synopsis.dht.store.entity.journal;

import sustain.synopsis.dht.journal.Activity;
import sustain.synopsis.dht.store.entity.journal.activity.EndSessionActivity;
import sustain.synopsis.dht.store.entity.journal.activity.SerializeSSTableActivity;
import sustain.synopsis.dht.store.entity.journal.activity.StartSessionActivity;

import java.io.IOException;
import java.nio.ByteBuffer;

public class JournalLogFactory {
    public static Activity parse(byte[] serialized) throws IOException, JournalingException {
        short type = ByteBuffer.wrap(serialized, 0, Short.BYTES).getShort();
        Activity activity;
        switch(type){
            case StartSessionActivity.TYPE:
                activity = new StartSessionActivity();
                break;
            case SerializeSSTableActivity.TYPE:
                activity = new SerializeSSTableActivity();
                break;
            case EndSessionActivity.TYPE:
                activity = new EndSessionActivity();
                break;
            default:
                throw new JournalingException("Unsupported record type: " + type);
        }
        activity.deserialize(serialized);
        return activity;
    }
}
