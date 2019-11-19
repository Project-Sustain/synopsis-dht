package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class SSTable<K extends Comparable<K> & Serializable, V extends Serializable> {

    private final Logger logger = Logger.getLogger(SSTable.class);
    private TableIterator<K, V>[] iterators;
    private int blockSize;

    public SSTable(int blockSize, TableIterator<K, V>... iterators) {
        this.blockSize = blockSize;
        this.iterators = iterators;
    }

    public SSTable() {

    }

    public void serialize(DataOutputStream blockOutputStream, DataOutputStream indexOutputStream) throws IOException {
        SortedMergeIterator<K, V> mergeIterator = new SortedMergeIterator<>(Arrays.asList(iterators));
        long totalSize = mergeIterator.estimatedSize();
        long totalEntryCount = mergeIterator.count();
        int blockCount = getUnitCount(totalSize, blockSize);
        long entriesPerBlock = getUnitCount(totalEntryCount, blockCount);

        blockOutputStream.writeInt(blockCount);
        indexOutputStream.writeInt(blockCount);
        for (int currentBlock = 0; currentBlock < blockCount; blockCount++) {
            long entryCountForCurrentBlock = entryCountForCurrentBlock(totalEntryCount, entriesPerBlock, currentBlock);
            blockOutputStream.writeLong(entryCountForCurrentBlock); // entries per current block
            for (int recordCount = 0; recordCount < entryCountForCurrentBlock; recordCount++) {
                if (mergeIterator.hasNext()) {
                    TableIterator.TableEntry<K, V> entry = mergeIterator.next();
                    // update the index
                    if (recordCount == 0) {
                        entry.getKey().serialize(indexOutputStream);
                        indexOutputStream.writeInt(blockOutputStream.size());
                    }
                    entry.getKey().serialize(blockOutputStream);
                    entry.getValue().serialize(blockOutputStream);
                }
            }
        }
    }

    int getUnitCount(long totalEntryCount, int entriesPerUnit) {
        if (totalEntryCount == 0) {    // if there are no entries, there should still be a single unit
            return 1;
        }
        return (int) Math.ceil(totalEntryCount * 1.0 / entriesPerUnit);
    }

    long entryCountForCurrentBlock(long totalEntries, long entriesPerBlock, int currentBlock) {
        if (totalEntries < entriesPerBlock) {
            return totalEntries;
        }
        return Math.min((totalEntries - currentBlock * entriesPerBlock), entriesPerBlock);
    }

    public void deserialize(DataInputStream inputStream, Class<K> keyClazz, Class<V> valueClazz) throws IOException {
        /*int entryCount = inputStream.readInt();
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
        }*/
    }
}
