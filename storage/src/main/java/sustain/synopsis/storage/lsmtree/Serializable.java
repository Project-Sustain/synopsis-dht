package sustain.synopsis.storage.lsmtree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Serializable {
    void serialize(DataOutputStream dataOutputStream) throws IOException;
    void deserialize(DataInputStream dataInputStream) throws IOException;
}
