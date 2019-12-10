package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.*;
import java.util.*;

class SSTableWriterTest {

    private static final int SEED = 123;

    @Test
    void testSerialize() throws IOException {
        // serialized size of an entry is 20 Bytes
        Metadata<LSMTestKey> metadata = new Metadata<>();
        byte[] serialized = getSerializedSSTable(metadata, null, null);

        // check the metadata
        Assertions.assertEquals(new LSMTestKey(0), metadata.getMin());
        Assertions.assertEquals(new LSMTestKey(100), metadata.getMax());
        Map<LSMTestKey, Integer> blockIndex = metadata.getBlockIndex();
        Assertions.assertEquals(5, blockIndex.size());

        // check if the data is serialized correctly
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        Random random = new Random(123);
        int blockCount = 0;
        int i = 0;

        boolean hasMoreBlocks = true;
        while (hasMoreBlocks) {
            hasMoreBlocks = dis.readBoolean();
            dis.readBoolean(); // compression status
            byte[] block = new byte[dis.readInt()];
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
            dis.readBoolean();
            dis.readBoolean(); // compression status
            byte[] block = new byte[dis.readInt()];
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
        writer.serialize(dos, metadata, null, null);
        dos.flush();
        baos.flush();

        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        boolean haveMoreBlocks = dis.readBoolean();
        boolean isCompressed = dis.readBoolean(); // compression status
        int size = dis.readInt();
        // there should not be other blocks
        Assertions.assertEquals(dis.available(), size);
        Assertions.assertFalse(haveMoreBlocks);
        Assertions.assertFalse(isCompressed);
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
        writer.serialize(dos, metadata, null, null);
        dos.flush();
        baos.flush();

        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        boolean haveMoreBlocks = dis.readBoolean();
        boolean isCompressed = dis.readBoolean();
        int size = dis.readInt();
        // there should not be other blocks
        Assertions.assertEquals(0, size);
        Assertions.assertFalse(isCompressed);
        Assertions.assertFalse(haveMoreBlocks);
        Assertions.assertEquals(0, metadata.getBlockIndex().size());
    }

    @Test
    void testSerializationWithCompression() throws IOException {
        // serialized size of an entry is 20 Bytes
        BlockCompressor compressor = new LZ4BlockCompressor();
        Metadata<LSMTestKey> metadata = new Metadata<>();
        byte[] serialized = getSerializedSSTable(metadata, compressor, null);
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);
        Iterator<LSMTestKey> blockIndexIterator = metadata.getBlockIndex().keySet().iterator();

        Random random = new Random(SEED);
        int i = 0;
        while (blockIndexIterator.hasNext()) {
            // checks if the block index is updated properly when the compression is enabled.
            dis.skipBytes(metadata.getBlockIndex().get(blockIndexIterator.next()));
            dis.readBoolean();
            boolean isCompressed = dis.readBoolean();
            Assertions.assertTrue(isCompressed);
            int decompressedLength = dis.readInt();
            int compressedLength = dis.readInt();
            byte[] block = new byte[compressedLength];
            dis.readFully(block);
            byte[] decompressed = compressor.decompress(decompressedLength, block);
            // check if the data is valid
            DataInputStream blockInputStream = new DataInputStream(new ByteArrayInputStream(decompressed));
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
            dis.reset(); // offsets are cumulative. so it is required to reset the input stream counter
        }
    }

    @Test
    void testSerializationWithChecksum() throws ChecksumGenerator.ChecksumError, IOException {
        Metadata<LSMTestKey> metadata = new Metadata<>();
        ChecksumGenerator checksumGenerator = new ChecksumGenerator();
        byte[] serialized = getSerializedSSTable(metadata, null, checksumGenerator);
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        Map<LSMTestKey, Integer> blockIndex = metadata.getBlockIndex();
        Map<LSMTestKey, byte[]> checksums = metadata.getChecksums();

        Assertions.assertEquals(blockIndex.size(), checksums.size());

        for (LSMTestKey key : blockIndex.keySet()) {
            int offset = blockIndex.get(key);
            dis.skipBytes(offset);
            // read the block
            dis.readBoolean();
            dis.readBoolean();
            byte[] block = new byte[dis.readInt()];
            dis.readFully(block);
            byte[] checksum = checksumGenerator.calculateChecksum(block);
            Assertions.assertTrue(Arrays.equals(checksum, checksums.get(key)));
            dis.reset();
        }
    }

    private byte[] getSerializedSSTable(Metadata<LSMTestKey> metadata, BlockCompressor compressor,
                                        ChecksumGenerator checksumGenerator) throws IOException {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 101, 20,
                        new Random(SEED))));
        SSTableWriter<LSMTestKey, LSMTestValue> ssTableWriter = new SSTableWriter<>(500,
                Collections.singletonList(mergeIter));
        // total data size = 20 * 101 = 2020
        // block count = 5
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        ssTableWriter.serialize(dos, metadata, compressor, checksumGenerator);
        dos.flush();
        byteArrayOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }
}
