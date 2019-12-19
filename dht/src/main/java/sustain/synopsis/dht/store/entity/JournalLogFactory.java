package sustain.synopsis.dht.store.entity;

import sustain.synopsis.dht.store.IngestionSession;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class JournalLogFactory {
    public void getStartSessionLog(IngestionSession session){
        try(ByteArrayOutputStream baos = new ByteArrayOutputStream(); DataOutputStream dataOutputStream =
                new DataOutputStream(baos)){

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
