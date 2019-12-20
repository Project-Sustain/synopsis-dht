package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;

class BlockProcessingTest {

    private byte[] block;

    @BeforeEach
    void setUp() {
        block = new byte[1024];
        Random random = new Random(123);
        random.nextBytes(block);
    }

    @Test
    void testLZ4Compression() {
        BlockCompressor lz4Compressor = new LZ4BlockCompressor();
        byte[] compressed = lz4Compressor.compress(block);
        byte[] decompressed = lz4Compressor.decompress(block.length, compressed);
        Assertions.assertTrue(Arrays.equals(block, decompressed));
    }

    @Test
    void testCheckSumGenerationAndValidation() throws ChecksumGenerator.ChecksumError {
        ChecksumGenerator checksumGenerator = new ChecksumGenerator();
        byte[] checksum = checksumGenerator.calculateChecksum(block);
        Assertions.assertTrue(checksumGenerator.validateChecksum(block, checksum));
        // modify the checksum
        byte[] modifiedChecksum = modifyByteArray(checksum);
        Assertions.assertFalse(checksumGenerator.validateChecksum(block, modifiedChecksum));
        // modify the data
        byte[] modifiedData = modifyByteArray(block);
        Assertions.assertFalse(checksumGenerator.validateChecksum(modifiedData, checksum));
    }

    private byte[] modifyByteArray(byte[] data) {
        byte[] modified = Arrays.copyOfRange(data, 0, data.length);
        int mask = 1;
        for (int i = 0; i < (Integer.SIZE - Byte.SIZE); i++) {
            mask |= mask << 1;
        }
        modified[0] = (byte) (~modified[0] & mask);
        return modified;
    }

    @Test
    void testMetadataSerialization() throws IOException, InstantiationException, IllegalAccessException {
        Metadata<LSMTestKey> metadata = new Metadata<>();
        metadata.setMin(new LSMTestKey(1));
        metadata.setMax(new LSMTestKey(10));
        metadata.addBlockIndex(new LSMTestKey(1), 0);
        metadata.addBlockIndex(new LSMTestKey(5), 50);
        metadata.addBlockIndex(new LSMTestKey(8), 80);

        Random random = new Random(123);
        byte[] checksum1 = new byte[10];
        byte[] checksum2 = new byte[10];
        byte[] checksum3 = new byte[10];
        random.nextBytes(checksum1);
        random.nextBytes(checksum2);
        random.nextBytes(checksum3);
        metadata.addChecksum(new LSMTestKey(1), checksum1);
        metadata.addChecksum(new LSMTestKey(5), checksum2);
        metadata.addChecksum(new LSMTestKey(8), checksum3);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        metadata.serialize(dos);
        dos.flush();
        baos.flush();

        byte[] serializedMetadata = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedMetadata);
        DataInputStream dis = new DataInputStream(bais);
        Metadata<LSMTestKey> deserialized = new Metadata<>();
        deserialized.deserialize(dis, LSMTestKey.class);

        Assertions.assertEquals(metadata.getMin(), deserialized.getMin());
        Assertions.assertEquals(metadata.getMax(), deserialized.getMax());
        Assertions.assertEquals(metadata.getBlockIndex(), deserialized.getBlockIndex());
        Map<LSMTestKey, byte[]> deserializedChecksums = deserialized.getChecksums();
        // we need to do compare byte[] individually because assertEquals on the map instance does not compare array
        // contents
        Assertions.assertEquals(3, deserializedChecksums.size());
        Assertions.assertArrayEquals(checksum1, deserializedChecksums.get(new LSMTestKey(1)));
        Assertions.assertArrayEquals(checksum2, deserializedChecksums.get(new LSMTestKey(2)));
        Assertions.assertArrayEquals(checksum3, deserializedChecksums.get(new LSMTestKey(3)));
    }
}
