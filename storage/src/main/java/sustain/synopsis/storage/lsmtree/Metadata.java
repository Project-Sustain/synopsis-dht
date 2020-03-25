package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class Metadata<K extends Comparable<K> & StreamSerializable> {
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
    private NavigableMap<K, Integer> blockIndex = new TreeMap<>();

    /**
     * Checksums for each block
     */
    private NavigableMap<K, byte[]> checksums = new TreeMap<>();

    /**
     * On-disk storage path
     */
    private String path;

    private long sessionId;

    private String user;

    private long sessionStartTS;

    private boolean isSessionComplete = false;

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

    public NavigableMap<K, Integer> getBlockIndex() {
        return blockIndex;
    }

    public Map<K, byte[]> getChecksums() {
        return checksums;
    }

    public void addChecksum(K key, byte[] checksum) {
        checksums.put(key, checksum);
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public boolean isSessionComplete() {
        return isSessionComplete;
    }

    public void setSessionComplete(boolean sessionComplete) {
        isSessionComplete = sessionComplete;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public long getSessionStartTS() {
        return sessionStartTS;
    }

    public void setSessionStartTS(long sessionStartTS) {
        this.sessionStartTS = sessionStartTS;
    }

    public void serialize(DataOutputStream dataOutputStream) throws IOException {
        min.serialize(dataOutputStream);
        max.serialize(dataOutputStream);
        dataOutputStream.writeUTF(path);
        dataOutputStream.writeLong(sessionId);
        dataOutputStream.writeBoolean(isSessionComplete);
        dataOutputStream.writeUTF(user);
        dataOutputStream.writeLong(sessionStartTS);
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
    }

    public void deserialize(DataInputStream dataInputStream, Class<K> clazz) throws IOException {
        try {
            this.min = clazz.newInstance();
            min.deserialize(dataInputStream);
            this.max = clazz.newInstance();
            max.deserialize(dataInputStream);
            this.path = dataInputStream.readUTF();
            this.sessionId = dataInputStream.readLong();
            this.isSessionComplete = dataInputStream.readBoolean();
            this.user = dataInputStream.readUTF();
            this.sessionStartTS = dataInputStream.readLong();
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
        } catch (InstantiationException | IllegalAccessException e) {
            logger.error("Error instantiating key instance.", e);
            throw new IOException(e);
        }
    }

    @Override
    public String toString() {
        return "Metadata{" + "min=" + min + ", max=" + max + ", path='" + path + '\'' + '}';
    }
}
