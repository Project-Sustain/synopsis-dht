package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.util.Arrays;
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
}
