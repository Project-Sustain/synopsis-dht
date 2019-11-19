package sustain.synopsis.storage.lsmtree;

import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Given a set of {@link TableIterator} instances, merge them into a single iterator while ensuring the elements
 * are returned in a sorted order. This class implements a K-way merge among the already sorted table iterators.
 * Uses a min heap to keep the minimum element from each iterator at a given moment. Handles iterators of different
 * sizes. Space complexity is O(k) where k is the number of iterators. Time complexity of next() is O(n log(n)) where
 * n is the total number of elements from all table iterators.
 * This implementation is not thread-safe.
 *
 * @param <K> key type that extend {@link Comparable}. All iterators should provide same type of keys.
 * @param <V> value type.
 */
class SortedMergeIterator<K extends Comparable<K>, V> implements TableIterator<K, V> {

    private class TableEntryWrapper implements Comparable<TableEntryWrapper> {
        private TableEntry<K, V> entry;
        private int iteratorId;

        TableEntryWrapper(TableEntry<K, V> entry, int iteratorId) {
            this.entry = entry;
            this.iteratorId = iteratorId;
        }

        @Override
        public int compareTo(TableEntryWrapper o) {
            return entry.getKey().compareTo(o.entry.getKey());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TableEntryWrapper that = (TableEntryWrapper) o;
            return Objects.equals(entry.getKey(), that.entry.getKey());
        }

        @Override
        public int hashCode() {
            return Objects.hash(entry.getKey());
        }
    }

    private final List<TableIterator<K, V>> iterators;
    private final PriorityQueue<TableEntryWrapper> minHeap;
    private boolean initialized;

    SortedMergeIterator(List<TableIterator<K, V>> iterators) {
        this.iterators = iterators;
        minHeap = new PriorityQueue<>(iterators.size());
        initialized = false;
    }

    private void initialize() {
        for (int i = 0; i < iterators.size(); i++) {
            TableIterator<K, V> iter = iterators.get(i);
            if (iter.hasNext()) {
                minHeap.add(new TableEntryWrapper(iter.next(), i));
            }
        }
    }

    @Override
    public boolean hasNext() {
        if (!initialized) {
            initialize();
            this.initialized = true;
        }
        return !minHeap.isEmpty();
    }

    @Override
    public TableEntry<K, V> next() {
        if (!initialized) {
            initialize();
            initialized = true;
        }
        TableEntryWrapper wrapper = minHeap.remove();
        TableIterator<K, V> iterator = iterators.get(wrapper.iteratorId);
        // replace the removed element with the next element from the same table iterator
        if (iterator.hasNext()) {
            minHeap.add(new TableEntryWrapper(iterator.next(), wrapper.iteratorId));
        }
        return wrapper.entry;
    }
}
