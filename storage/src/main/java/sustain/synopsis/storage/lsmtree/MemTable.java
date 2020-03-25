package sustain.synopsis.storage.lsmtree;

import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public class MemTable<K extends Comparable<K> & StreamSerializable, V extends StreamSerializable & Mergeable<V>> {

    private static final int DEFAULT_MAX_ENTRY_COUNT = 100;
    private Logger logger = Logger.getLogger(MemTable.class);
    /**
     * This is a counter measure in case estimating the serialized size of a memtable fails.
     * In such cases, set the upper bound in terms of the number of entries.
     */
    private final int maxEntryCount;
    private long maxSizeInBytes;
    private int estimatedEntrySize = -1;

    private NavigableMap<K, V> elements = new TreeMap<>();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public MemTable(long maxSizeInBytes) {
        this(maxSizeInBytes, DEFAULT_MAX_ENTRY_COUNT);
    }

    public MemTable(long maxSizeInBytes, int maxEntryCount) {
        this.maxSizeInBytes = maxSizeInBytes;
        this.maxEntryCount = maxEntryCount;
    }

    public boolean add(K key, V value) {
        try {
            lock.writeLock().lock();
            if (elements.containsKey(key)) {
                elements.get(key).merge(value);
            } else {
                elements.put(key, value);
            }
            // estimate the size of the first entry - used to estimate the size of the memTable
            if (estimatedEntrySize == -1) {
                try {
                    estimatedEntrySize = estimateEntrySize(key, value);
                } catch (IOException e) {
                    logger.error("Error estimating the entry size.", e);
                }
            }
            boolean isMemTableFull = isMemTableFull();
            if (isMemTableFull && logger.isTraceEnabled()) {
                logger.trace("Estimated entry size: " + estimatedEntrySize + ", Element Count: " + elements.size() +
                        ", " + "Threshold: " + maxSizeInBytes);
            }
            return isMemTableFull;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private int estimateEntrySize(K key, V value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            key.serialize(dos);
            value.serialize(dos);
            dos.flush();
            return dos.size();
        } finally {
            baos.close();
            dos.close();
        }
    }

    private boolean isMemTableFull() {
        if (estimatedEntrySize > 0) {
            return estimatedEntrySize * elements.size() >= maxSizeInBytes;
        } else { // this is a corner case - if the estimated entry size keeps on failing
            return elements.size() >= maxEntryCount;
        }
    }

    public long getEstimatedSize() {
        return estimatedEntrySize * elements.size();
    }

    public TableIterator<K, V> getIterator() {
        try {
            lock.readLock().lock();
            return new TableIterator<K, V>() {

                Iterator<K> iterator = elements.navigableKeySet().iterator();

                @Override
                public boolean hasNext() {
                    return iterator.hasNext();
                }

                @Override
                public TableEntry<K, V> next() {
                    K key = iterator.next();
                    return new TableEntry<>(key, elements.get(key));
                }
            };
        } finally {
            lock.readLock().unlock();
        }
    }

    public K getFirstKey() {
        if (elements.size() > 0) {
            return elements.firstKey();
        }
        return null;
    }

    public K getLastKey() {
        if (elements.size() > 0) {
            return elements.lastKey();
        }
        return null;
    }

    public int getEntryCount() {
        return elements.size();
    }

    public void clear() {
        elements.clear();
    }
}
