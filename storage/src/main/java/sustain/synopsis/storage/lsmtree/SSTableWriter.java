package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class SSTableWriter<K extends Comparable<K> & Serializable, V extends Serializable> {

    private final Logger logger = Logger.getLogger(SSTableWriter.class);
    private List<TableIterator<K, V>> iterators;
    private int blockSize;

    public SSTableWriter(int blockSize, List<TableIterator<K, V>> iterators) {
        this.blockSize = blockSize;
        this.iterators = iterators;
    }

    public void serialize(DataOutputStream blockOutputStream, Metadata<K> metadata) throws IOException {
        SortedMergeIterator<K, V> mergeIterator = new SortedMergeIterator<>(iterators);
        ByteArrayOutputStream baos = new ByteArrayOutputStream(blockSize);
        DataOutputStream dos = new DataOutputStream(baos);
        Map<K, Integer> blockIndex = new TreeMap<>();
        K minKey = null;
        K maxKey = null;
        int blockCount = 0;

        while (mergeIterator.hasNext()) {
            TableIterator.TableEntry<K, V> entry = mergeIterator.next();

            // update the block index
            if (dos.size() == 0) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Updated block index. Block Id: " + blockCount + ", Offset: " + blockOutputStream.size());
                }
                blockIndex.put(entry.getKey(), blockOutputStream.size());
            }
            // limits of the SSTable
            if (minKey == null) {
                minKey = entry.getKey();
            }
            maxKey = entry.getKey();

            entry.getKey().serialize(dos);
            entry.getValue().serialize(dos);
            if (dos.size() >= blockSize) {
                // it is possible to exceed the block size because we are not splitting an entry between two blocks
                // current block is full
                dos.flush();
                baos.flush();
                byte[] currentBlock = baos.toByteArray();
                blockOutputStream.writeInt(currentBlock.length);
                blockOutputStream.writeBoolean(mergeIterator.hasNext()); // there is a block after the current block
                blockOutputStream.write(currentBlock);
                baos.reset();
                dos.close();
                dos = new DataOutputStream(baos);
                if (logger.isDebugEnabled()) {
                    logger.debug("Stored block " + blockCount + " on disk. Block Size: " + currentBlock.length);
                }
                blockCount++;
            }
        }
        if (dos.size() > 0) {
            // last block
            byte[] lastBlock = baos.toByteArray();
            blockOutputStream.writeInt(lastBlock.length);
            blockOutputStream.writeBoolean(false);
            blockOutputStream.write(lastBlock);
            if (logger.isDebugEnabled()) {
                logger.debug("Stored the final block of the SSTable on disk. Block Id: " + blockCount + ", block " +
                        "size: " + lastBlock.length);
            }
        } else if (blockCount == 0) { // empty SSTable
            blockOutputStream.writeInt(0);
            blockOutputStream.writeBoolean(false);
            if (logger.isDebugEnabled()) {
                logger.debug("Stored empty SSTable on disk.");
            }
        }

        // set the metadata
        metadata.setMin(minKey);
        metadata.setMax(maxKey);
        metadata.setBlockIndex(blockIndex);
    }

    /*public void deserialize(DataInputStream inputStream, Class<K> keyClazz, Class<V> valueClazz) throws IOException {
        int entryCount = inputStream.readInt();
        for (int i = 0; i < entryCount; i++) {
            try {
                K key = keyClazz.newInstance();
                key.deserialize(inputStream);
                V val = valueClazz.newInstance();
                val.deserialize(inputStream);
                elements.put(key, val);
            } catch (InstantiationException | IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }*/
}
