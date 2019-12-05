package sustain.synopsis.storage.lsmtree.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * LZ4 based compression/decompression for blocks
 */
public class LZ4BlockCompressor implements BlockCompressor {

    private final LZ4Compressor compressor;
    private final LZ4FastDecompressor decompressor;

    public LZ4BlockCompressor() {
        LZ4Factory factory = LZ4Factory.fastestJavaInstance();
        compressor = factory.fastCompressor();
        decompressor = factory.fastDecompressor();
    }

    @Override
    public byte[] compress(byte[] block) {
        return compressor.compress(block);
    }

    @Override
    public byte[] decompress(int decompresseLength, byte[] compressed) {
        byte[] decompressed = new byte[decompresseLength];
        decompressor.decompress(compressed, decompressed);
        return decompressed;
    }
}
