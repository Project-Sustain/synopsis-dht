package synopsis2.dht.store.halodb;

import com.oath.halodb.HaloDBIterator;
import com.oath.halodb.Record;
import synopsis2.dht.store.Iterator;
import synopsis2.dht.store.StoredEntry;

public class HaloDBStoreIterator implements Iterator {
    private final HaloDBIterator iterator;

    public HaloDBStoreIterator(HaloDBIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public StoredEntry next() {
        Record rec = iterator.next();
        return new StoredEntry(rec.getKey(), rec.getValue());
    }
}
