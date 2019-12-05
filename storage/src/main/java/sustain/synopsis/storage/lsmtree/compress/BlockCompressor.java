package sustain.synopsis.storage.lsmtree.compress;

/**
 * Provides compression/decompression for a block
 */
public interface BlockCompressor {

    /**
     * Compress the provided block
     * @param block Decompressed block
     * @return Compressed block
     */
    byte[] compress(byte[] block);

    /**
     * Decompress a compressed block
     * @param decompressedLength Decompressed length. This is required by most of the compression libraries.
     *                           Providing this can avoid trial and error style buffer allocation, therefore can
     *                           improve the long tail of the decompression time.
     * @param compressed Compressed data
     * @return Decompressed data
     */
    byte[] decompress(int decompressedLength, byte[] compressed);
}
