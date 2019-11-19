package sustain.synopsis.storage.lsmtree;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class Metadata<K> implements Serializable{
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
    private Map<K,Integer> blockIndex;

    public void setMin(K min) {
        this.min = min;
    }

    public void setMax(K max) {
        this.max = max;
    }

    public void setBlockIndex(Map<K, Integer> blockIndex) {
        this.blockIndex = blockIndex;
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

    @Override
    public void serialize(DataOutputStream dataOutputStream) throws IOException {

    }

    @Override
    public void deserialize(DataInputStream dataInputStream) throws IOException {

    }
}
