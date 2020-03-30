package sustain.synopsis.storage.lsmtree;

/**
 * Provides a sorted iterator over a collection of key,value pairs.
 * @param <K> Key type that extend {@link Comparable}
 * @param <V> Value type
 */
public interface TableIterator<K extends Comparable<K>, V> {
    class TableEntry<K, V> {
        private K key;
        private V value;

        public TableEntry(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }

    /**
     * Returns true if there are one or more elements remaining in the iterator
     * @return <code>true</code> if there are one or more element remaining in the iterator
     */
    boolean hasNext();

    /**
     * Returns the pair of next key and the corresponding value
     * @return {@link TableEntry} element with next key and corresponding value
     */
    TableEntry<K, V> next();
}
