package sustain.synopsis.storage.lsmtree.compress;

public class CompressorFactory {

    public enum Compressor{
        LZ4, SNAPPY;
    }

    public static BlockCompressor getCompressor(Compressor c){
        return null;
    }
}
