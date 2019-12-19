package sustain.synopsis.storage.lsmtree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Metadata<K extends Comparable<K>> implements Serializable {
    /**
     * Minimum key of a SSTable
     */
    private K min;

    /**
     * Maximum key of a SSTable
     */
    private K max;

    /**
     * Index of the offset for each block and the first key of the block
     */
    private Map<K, Integer> blockIndex = new TreeMap<>();
    private Map<K, byte[]> checksums = new TreeMap<>();

    public void setMin(K min) {
        this.min = min;
    }

    public void setMax(K max) {
        this.max = max;
    }

    public void addBlockIndex(K key, int offset) {
        blockIndex.put(key, offset);
    }

    public K getMin() {
        return min;
    }

    public K getMax() {
        return max;
    }

    public Map<K, Integer> getBlockIndex() {
        return blockIndex;
    }

    public Map<K, byte[]> getChecksums() {
        return checksums;
    }

    public void addChecksum(K key, byte[] checksum) {
        checksums.put(key, checksum);
    }

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {

    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {

    }
}
