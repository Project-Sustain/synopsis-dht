package sustain.synopsis.storage.lsmtree;

import com.sun.istack.internal.NotNull;

import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

class MergeIterator<K extends Comparable<K>, V> implements TableIterator<K, V> {

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

    MergeIterator(List<TableIterator<K, V>> iterators) {
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
        if (iterator.hasNext()) {
            minHeap.add(new TableEntryWrapper(iterator.next(), wrapper.iteratorId));
        }
        return wrapper.entry;
    }

    @Override
    public long count() {
        return iterators.stream().map(TableIterator::count).reduce(0L, Long::sum);
    }

    @Override
    public long estimatedSize() {
        return iterators.stream().map(TableIterator::estimatedSize).reduce(0L, Long::sum);
    }
}
