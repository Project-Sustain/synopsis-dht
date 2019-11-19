package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.util.Collections;
import java.util.Map;
import java.util.Random;

class SSTableWriterTest {

    @Test
    void testSerialize() throws IOException {
        // serialized size of an entry is 20 Bytes
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 101, 20,
                        new Random(123))));
        SSTableWriter<LSMTestKey, LSMTestValue> ssTableWriter = new SSTableWriter<>(500,
                Collections.singletonList(mergeIter));
        // total data size = 20 * 101 = 2020
        // block count = 5
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        Metadata<LSMTestKey> metadata = new Metadata<>();
        ssTableWriter.serialize(dos, metadata);
        dos.flush();
        byteArrayOutputStream.flush();

        // check the metadata
        Assertions.assertEquals(new LSMTestKey(0), metadata.getMin());
        Assertions.assertEquals(new LSMTestKey(100), metadata.getMax());
        Map<LSMTestKey, Integer> blockIndex = metadata.getBlockIndex();
        Assertions.assertEquals(5, blockIndex.size());

        // check if the data is serialized correctly
        byte[] serialized = byteArrayOutputStream.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        Random random = new Random(123);
        int blockCount = 0;
        int i = 0;

        boolean hasMoreBlocks = true;
        while (hasMoreBlocks) {
            byte[] block = new byte[dis.readInt()];
            hasMoreBlocks = dis.readBoolean();
            dis.readFully(block);
            blockCount++;
            DataInputStream blockInputStream = new DataInputStream(new ByteArrayInputStream(block));
            while (blockInputStream.available() > 0) {
                LSMTestKey k = new LSMTestKey();
                LSMTestValue v = new LSMTestValue();
                k.deserialize(blockInputStream);
                v.deserialize(blockInputStream);
                Assertions.assertEquals(new LSMTestKey(i++), k);
                byte[] expectedVal = new byte[12]; // total entry size is 20. (key -4, length encoding - 4, payload -
                // 12)
                random.nextBytes(expectedVal); // payloads are generated in a deterministic ways using the same seed
                Assertions.assertEquals(new LSMTestValue(expectedVal), v);
            }
        }
        Assertions.assertEquals(5, blockCount);

        // check if the index is correctly generated.
        for (Map.Entry<LSMTestKey, Integer> entry : blockIndex.entrySet()) {
            dis = new DataInputStream(new ByteArrayInputStream(serialized));
            dis.skipBytes(entry.getValue());
            byte[] block = new byte[dis.readInt()];
            dis.readBoolean();
            dis.readFully(block);
            DataInputStream blockInputStream = new DataInputStream(new ByteArrayInputStream(block));
            LSMTestKey testKey = new LSMTestKey();
            testKey.deserialize(blockInputStream);
            Assertions.assertEquals(entry.getKey(), testKey);
        }
    }

    @Test
    void testSerializationWithExactBlockFill() throws IOException {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 10, 20,
                        new Random(123))));
        SSTableWriter<LSMTestKey, LSMTestValue> writer = new SSTableWriter<>(200, Collections.singletonList(mergeIter));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Metadata<LSMTestKey> metadata = new Metadata<>();
        writer.serialize(dos, metadata);
        dos.flush();
        baos.flush();

        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        int size = dis.readInt();
        boolean haveMoreBlocks = dis.readBoolean();
        // there should not be other blocks
        Assertions.assertEquals(dis.available(), size);
        Assertions.assertFalse(haveMoreBlocks);
        Assertions.assertEquals(1, metadata.getBlockIndex().size());
    }

    @Test
    void testSerializationEmptySSTable() throws IOException {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 0, 20,
                        new Random(123))));
        SSTableWriter<LSMTestKey, LSMTestValue> writer = new SSTableWriter<>(200, Collections.singletonList(mergeIter));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Metadata<LSMTestKey> metadata = new Metadata<>();
        writer.serialize(dos, metadata);
        dos.flush();
        baos.flush();

        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        int size = dis.readInt();
        boolean haveMoreBlocks = dis.readBoolean();
        // there should not be other blocks
        Assertions.assertEquals(0, size);
        Assertions.assertFalse(haveMoreBlocks);
        Assertions.assertEquals(0, metadata.getBlockIndex().size());
    }
}
