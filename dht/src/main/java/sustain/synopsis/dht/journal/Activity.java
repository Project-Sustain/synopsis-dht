package sustain.synopsis.dht.journal;

import java.io.IOException;

/**
 * An activity that gets journaled using {@link Logger}.
 */
public interface Activity {
    /**
     * A unique identifier for the type of the activity
     *
     * @return Identifier of type {@link Short}.
     */
    public short getType();

    /**
     * Serialize the activity
     *
     * @return <code>byte[]</code> of the serialized data
     * @throws IOException I/O error during serialization
     */
    public byte[] serialize() throws IOException;

    /**
     * Deserialize the activity
     *
     * @param serializedData serialized data provided as a <code>byte[]</code>
     * @throws IOException I/O error during deserialization
     */
    public void deserialize(byte[] serializedData) throws IOException;
}

