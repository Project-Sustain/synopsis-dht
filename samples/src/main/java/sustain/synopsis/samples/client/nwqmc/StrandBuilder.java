package sustain.synopsis.samples.client.nwqmc;

import sustain.synopsis.ingestion.client.core.Record;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StrandBuilder {

    final Set<RecordType> requiredRecords;
    final Set<RecordType> acquiredRecords;

    String geohash;
    Long fromTs;
    Long toTs;

    final Map<String, Float> features;

    public StrandBuilder(Set<RecordType> requiredRecords) {
        this.requiredRecords = requiredRecords;
        this.acquiredRecords = new HashSet<>(requiredRecords.size());
        this.features = new HashMap<>();
    }

    // TODO
    public boolean onDataWithRecordType(Map<String, Integer> columnMap, String[] splits, RecordType recordType) {
        return false;
    }

}
