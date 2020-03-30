package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.util.Random;

import static java.nio.file.StandardOpenOption.*;

public class UtilTest {
    @TempDir
    File tempDir;

    @Test
    void testReadFully() throws IOException {
        File file = new File(tempDir.getAbsolutePath() + File.separator + "test.file");
        final int PAYLOAD_SIZE = 1024 * 1024;
        byte[] writePayload = new byte[PAYLOAD_SIZE];
        new Random(123).nextBytes(writePayload);
        ByteBuffer writeBuffer = ByteBuffer.wrap(writePayload);

        // write the test payload to the file
        SeekableByteChannel writeChannel = FileChannel.open(file.toPath(), CREATE, WRITE);
        while (writeBuffer.hasRemaining()) {
            writeChannel.write(writeBuffer);
        }
        Assertions.assertEquals(PAYLOAD_SIZE, writeChannel.position());
        writeChannel.close();

        ByteBuffer readBuffer = ByteBuffer.allocate(PAYLOAD_SIZE);
        SeekableByteChannel readChannel = FileChannel.open(file.toPath(), READ);
        Util.readFully(readChannel, readBuffer);
        Assertions.assertArrayEquals(writePayload, readBuffer.array());

        // channel position is at the end of the file. should not read any data.
        readBuffer.clear();
        Util.readFully(readChannel, readBuffer);
        Assertions.assertEquals(0, readBuffer.position());
    }

    @Test
    void readBoolean() throws IOException {
        File file = new File(tempDir.getAbsolutePath() + File.separator + "test.file");
        ByteBuffer buffer = ByteBuffer.allocate(4 * Byte.SIZE/8);
        buffer.put((byte) 2);
        buffer.put((byte) 1);
        buffer.put((byte) 0);
        buffer.put((byte) -1);
        buffer.flip();
        // write the test payload to the file
        SeekableByteChannel writeChannel = FileChannel.open(file.toPath(), CREATE, WRITE);
        while (buffer.hasRemaining()) {
            writeChannel.write(buffer);
        }
        writeChannel.close();

        SeekableByteChannel readChannel = FileChannel.open(file.toPath(), READ);
        Assertions.assertTrue(Util.readBoolean(readChannel));
        Assertions.assertTrue(Util.readBoolean(readChannel));
        Assertions.assertFalse(Util.readBoolean(readChannel));
        Assertions.assertThrows(IllegalArgumentException.class, ()-> Util.readBoolean(readChannel));

        readChannel.close();
    }
}
