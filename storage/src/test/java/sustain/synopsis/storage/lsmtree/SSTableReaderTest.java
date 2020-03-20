package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.Random;

import static java.nio.file.StandardOpenOption.READ;

public class SSTableReaderTest {

    @TempDir
    File tempDir;

    @Test
    void testExtractBlockDataCompressed() throws IOException {
        byte[] data = new byte[1024 * 1024];
        new Random(1).nextBytes(data);
        BlockCompressor compressor = new LZ4BlockCompressor();
        byte[] compressedData = compressor.compress(data);

        File file = new File(tempDir.getAbsolutePath() + File.separator + "temp.file");
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream dos = new DataOutputStream(fos);
        dos.writeBoolean(false); // isLastBlock
        dos.writeBoolean(true); // isCompressed
        dos.writeInt(data.length);
        dos.writeInt(compressedData.length);
        dos.write(compressedData);
        dos.flush();
        fos.flush();
        dos.close();
        fos.close();

        FileChannel channel = FileChannel.open(file.toPath(), READ);
        // we do not need to properly initialize the SSTableReader
        SSTableReader<LSMTestKey> reader = new SSTableReader<>(null, null);
        byte[] readData = reader.extractBlockData(channel);
        Assertions.assertArrayEquals(data, readData);
    }

    @Test
    void testExtractBlockDataUncompressed() throws IOException {
        byte[] data = new byte[1024 * 1024];
        new Random(1).nextBytes(data);

        File file = new File(tempDir.getAbsolutePath() + File.separator + "temp.file");
        FileOutputStream fos = new FileOutputStream(file);
        DataOutputStream dos = new DataOutputStream(fos);
        dos.writeBoolean(false); // isLastBlock
        dos.writeBoolean(false); // isCompressed
        dos.writeInt(data.length);
        dos.write(data);
        dos.flush();
        fos.flush();
        dos.close();
        fos.close();

        FileChannel channel = FileChannel.open(file.toPath(), READ);
        // we do not need to properly initialize the SSTableReader
        SSTableReader<LSMTestKey> reader = new SSTableReader<>(null, null);
        byte[] readData = reader.extractBlockData(channel);
        Assertions.assertArrayEquals(data, readData);
    }

    @Test
    void testGetPairIterator() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Random random = new Random(1);
        byte[] payload = new byte[100];
        final int ELEM_COUNT = 10;
        for(int i = 0; i < ELEM_COUNT; i++) {
            LSMTestKey key = new LSMTestKey(i);
            random.nextBytes(payload);
            key.serialize(dos);
            dos.writeInt(payload.length);
            dos.write(payload);
        }
        dos.flush();
        baos.flush();
        byte[] block = baos.toByteArray();

        SSTableReader<LSMTestKey> reader = new SSTableReader<>(null, LSMTestKey.class);
        Iterator<SSTableReader.Pair<LSMTestKey>> iterator = reader.getPairIterator(block);
        int elemCount = 0;
        random = new Random(1); // reinitialize the random number generator
        while(iterator.hasNext()){
            SSTableReader.Pair<LSMTestKey> pair = iterator.next();
            Assertions.assertEquals(new LSMTestKey(elemCount++), pair.getK());
            random.nextBytes(payload);
            Assertions.assertArrayEquals(payload, pair.getData());
        }
        Assertions.assertEquals(ELEM_COUNT, elemCount);
    }
}
