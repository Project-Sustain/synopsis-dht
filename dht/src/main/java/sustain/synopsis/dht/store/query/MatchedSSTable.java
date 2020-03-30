package sustain.synopsis.dht.store.query;

import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.util.ArrayList;
import java.util.List;

/**
 * A helper class to hold the SSTables and the time intervals they are matched for.
 */
public class MatchedSSTable {
    private Metadata<StrandStorageKey> metadata;
    private List<Interval> matchedIntervals;

    public MatchedSSTable(Metadata<StrandStorageKey> metadata) {
        this.metadata = metadata;
        this.matchedIntervals = new ArrayList<>();
    }

    public void addMatchedInterval(Interval interval){
        matchedIntervals.add(interval);
    }

    public Metadata<StrandStorageKey> getMetadata() {
        return metadata;
    }

    public List<Interval> getMatchedIntervals() {
        return matchedIntervals;
    }
}
