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
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(file.getAbsolutePath());
        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);

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
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(file.getAbsolutePath());

        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);
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
        for (int i = 0; i < ELEM_COUNT; i++) {
            LSMTestKey key = new LSMTestKey(i);
            random.nextBytes(payload);
            key.serialize(dos);
            dos.writeInt(payload.length);
            dos.write(payload);
        }
        dos.flush();
        baos.flush();
        byte[] block = baos.toByteArray();

        // dummy file
        File file = new File(tempDir.getAbsolutePath() + File.separator + "temp.file");
        file.createNewFile();
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(file.getAbsolutePath());

        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);
        Iterator<TableIterator.TableEntry<LSMTestKey, LSMTestValue>> iterator = reader.getPairIterator(block);
        int elemCount = 0;
        random = new Random(1); // reinitialize the random number generator
        while (iterator.hasNext()) {
            TableIterator.TableEntry<LSMTestKey, LSMTestValue> entry = iterator.next();
            Assertions.assertEquals(new LSMTestKey(elemCount++), entry.getKey());
            random.nextBytes(payload);
            Assertions.assertArrayEquals(payload, entry.getValue().getVal());
        }
        Assertions.assertEquals(ELEM_COUNT, elemCount);
    }

    @Test
    void testGetPairIteratorWithEmptyBlock() throws IOException {
        // dummy file
        File file = new File(tempDir.getAbsolutePath() + File.separator + "temp.file");
        file.createNewFile();
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(file.getAbsolutePath());
        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);
        Iterator<TableIterator.TableEntry<LSMTestKey, LSMTestValue>> iterator = reader.getPairIterator(new byte[0]);
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testGetPairIteratorInstantiationError() throws IOException {
        class NoDefaultConstructor implements Comparable<NoDefaultConstructor>, StreamSerializable {

            NoDefaultConstructor(int param) {

            }

            @Override
            public int compareTo(NoDefaultConstructor o) {
                return 0;
            }

            @Override
            public void serialize(DataOutputStream dataOutputStream){

            }

            @Override
            public void deserialize(DataInputStream dataInputStream){

            }
        }

        // dummy file
        File file = new File(tempDir.getAbsolutePath() + File.separator + "temp.file");
        file.createNewFile();
        Metadata<NoDefaultConstructor> metadata = new Metadata<>();
        metadata.setPath(file.getAbsolutePath());
        SSTableReader<NoDefaultConstructor, LSMTestValue> reader =
                new SSTableReader<>(metadata, NoDefaultConstructor.class, LSMTestValue.class);
        Iterator<TableIterator.TableEntry<NoDefaultConstructor, LSMTestValue>> iterator =
                reader.getPairIterator(new byte[100]);
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testGetPairIteratorIOError() throws IOException {
        // dummy file
        File file = new File(tempDir.getAbsolutePath() + File.separator + "temp.file");
        file.createNewFile();
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(file.getAbsolutePath());
        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);
        Iterator<TableIterator.TableEntry<LSMTestKey, LSMTestValue>> iterator =
                reader.getPairIterator(new byte[1]); // not enough data to deserialize
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testReadBlock() throws IOException {
        final int BLOCK_COUNT = 3;
        final int ELEM_COUNT_PER_BLOCK = 10;

        File f = new File(tempDir + File.separator + "temp.file");
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(f.getAbsolutePath());
        BlockCompressor compressor = new LZ4BlockCompressor();
        FileOutputStream fos = new FileOutputStream(f);

        DataOutputStream dos = new DataOutputStream(fos);
        for (int blockId = 0; blockId < BLOCK_COUNT; blockId++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream blockDataOS = new DataOutputStream(baos);
            Random random = new Random(1);
            byte[] payload = new byte[100];
            for (int i = 0; i < ELEM_COUNT_PER_BLOCK; i++) {
                LSMTestKey key = new LSMTestKey(blockId * 10 + i);
                if (i == 0) {
                    // update the index
                    metadata.addBlockIndex(key, dos.size());
                }
                random.nextBytes(payload);
                key.serialize(blockDataOS);
                blockDataOS.writeInt(payload.length);
                blockDataOS.write(payload);
            }
            dos.flush();
            baos.flush();
            byte[] block = baos.toByteArray();
            byte[] compressedData = compressor.compress(block);
            dos.writeBoolean(blockId == (BLOCK_COUNT - 1));
            dos.writeBoolean(true); // compressed
            dos.writeInt(block.length);
            dos.writeInt(compressedData.length);
            dos.write(compressedData);
            dos.flush();
        }
        dos.close();
        fos.flush();
        fos.close();

        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);
        for (int blockId = 0; blockId < BLOCK_COUNT; blockId++) {
            LSMTestKey expectedFirstKey = new LSMTestKey(blockId * ELEM_COUNT_PER_BLOCK);
            Iterator<TableIterator.TableEntry<LSMTestKey, LSMTestValue>> iter = reader.readBlock(expectedFirstKey);
            // check the first key - we have tested the iterator content in testGetPairIterator()
            Assertions.assertTrue(iter.hasNext());
            TableIterator.TableEntry<LSMTestKey, LSMTestValue> firstEntry = iter.next();
            Assertions.assertEquals(expectedFirstKey, firstEntry.getKey());
        }
    }

    @Test
    void testReadBlockInValidIndex() throws IOException {
        File f = new File(tempDir + File.separator + "temp.file");
        f.createNewFile();
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setPath(f.getAbsolutePath());
        SSTableReader<LSMTestKey, LSMTestValue> reader =
                new SSTableReader<>(metadata, LSMTestKey.class, LSMTestValue.class);
        Assertions.assertThrows(IOException.class, () -> reader.readBlock(new LSMTestKey(1)));
    }
}
