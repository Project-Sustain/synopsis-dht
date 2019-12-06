package sustain.synopsis.dht.store.halodb;

import com.oath.halodb.HaloDBIterator;
import com.oath.halodb.Record;
import sustain.synopsis.dht.store.Iterator;
import sustain.synopsis.dht.store.StoredEntry;

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
