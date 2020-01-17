package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class Metadata<K extends Comparable<K> & Serializable> {
    private final Logger logger = Logger.getLogger(Metadata.class);
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

    /**
     * Checksums for each block
     */
    private Map<K, byte[]> checksums = new TreeMap<>();

    private String path;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

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

    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        min.serialize(dataOutputStream);
        max.serialize(dataOutputStream);
        // block index
        dataOutputStream.writeInt(blockIndex.size());
        for (K key : blockIndex.keySet()) {
            key.serialize(dataOutputStream);
            dataOutputStream.writeInt(blockIndex.get(key));
        }
        // checksums
        dataOutputStream.writeInt(checksums.size());
        for (K key : checksums.keySet()) {
            key.serialize(dataOutputStream);
            byte[] checksum = checksums.get(key);
            dataOutputStream.writeInt(checksum.length);
            dataOutputStream.write(checksum);
        }
        dataOutputStream.writeUTF(path);
    }

    public void deserialize(DataInputStream dataInputStream, Class<K> clazz) throws IOException {
        try {
            this.min = clazz.newInstance();
            min.deserialize(dataInputStream);
            this.max = clazz.newInstance();
            max.deserialize(dataInputStream);
            // block index
            int blockIndexSize = dataInputStream.readInt();
            for (int i = 0; i < blockIndexSize; i++) {
                K key = clazz.newInstance();
                key.deserialize(dataInputStream);
                blockIndex.put(key, dataInputStream.readInt());
            }
            // checksum
            int checksumCount = dataInputStream.readInt();
            for (int i = 0; i < checksumCount; i++) {
                K key = clazz.newInstance();
                key.deserialize(dataInputStream);
                byte[] checksum = new byte[dataInputStream.readInt()];
                dataInputStream.readFully(checksum);
                checksums.put(key, checksum);
            }
            this.path = dataInputStream.readUTF();
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error("Error instantiating key instance.", e);
            throw new IOException(e);
        }
    }
}
