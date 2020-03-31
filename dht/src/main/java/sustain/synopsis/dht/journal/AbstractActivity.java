package sustain.synopsis.dht.journal;

import java.io.*;

/**
 * A simplified version of the {@link Activity} by abstracting the common functionality.
 */
public abstract class AbstractActivity implements Activity {

    @Override
    public byte[] serialize() throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos);) {
            dos.writeShort(getType());
            serializeMembers(dos);
            dos.flush();
            baos.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public void deserialize(byte[] serializedData) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
             DataInputStream dis = new DataInputStream(bais);) {
            dis.readShort(); // read and ignore the type
            deserializeMembers(dis);
        }
    }

    /**
     * Serialize activity specific member variables
     *
     * @param dataOutputStream {@link DataOutputStream} object used for serialization
     * @throws IOException I/O error during serialization
     */
    public abstract void serializeMembers(DataOutputStream dataOutputStream) throws IOException;

    /**
     * Deserialize activity specific member variables
     *
     * @param dataInputStream {@link DataInputStream} used for deserialization
     * @throws IOException I/O error during deserialization
     */
    public abstract void deserializeMembers(DataInputStream dataInputStream) throws IOException;
}
