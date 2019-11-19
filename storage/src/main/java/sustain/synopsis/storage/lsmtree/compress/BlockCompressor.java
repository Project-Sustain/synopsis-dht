package sustain.synopsis.storage.lsmtree.compress;

public interface BlockCompressor {
    byte[] compress(byte[] block);
}
