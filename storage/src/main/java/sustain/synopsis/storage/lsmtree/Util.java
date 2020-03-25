package sustain.synopsis.storage.lsmtree;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public class Util {
    /**
     * Read fully from a {@link SeekableByteChannel}
     *
     * @param channel Channel from which the data is read
     * @param buffer  ByteBuffer into which the data is read in. This method does not reset the buffer
     *                position - data is read into the available space. Buffer is flipped at the end.
     * @throws IOException Error during the read
     */
    public static void readFully(SeekableByteChannel channel, ByteBuffer buffer) throws IOException {
        int readCount;
        do {
            readCount = channel.read(buffer);
        } while (readCount != -1 && buffer.hasRemaining());
        buffer.flip();
    }

    /**
     * Reads a boolean from a channel. Boolean values are assumed to be serialized as bytes similar to
     * {@link java.io.DataOutputStream}. Value 0 is considered <code>false</code>, and all other positive values
     * considered <code>true</code>.
     *
     * @param channel {@link SeekableByteChannel} from which the data is read
     * @return <code>true</code> if the value > 0, <code>false</code> if the value = 0
     * @throws IOException Error reading from the channel
     */
    public static boolean readBoolean(SeekableByteChannel channel) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(Byte.SIZE/8); // bits to byte conversion
        readFully(channel, buffer);
        byte b = buffer.get();
        if(b < 0){
            throw new IllegalArgumentException("Boolean values must be represented using 8 bit integers of >= 0. " +
                    "Value: " + b);
        }
        return b != 0;
    }
}
