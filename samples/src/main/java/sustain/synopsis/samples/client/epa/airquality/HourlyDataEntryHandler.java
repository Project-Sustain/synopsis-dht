package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.sketch.util.Geohash;

import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;

public interface HourlyDataEntryHandler {
    void onHourlyDataEntry(HourlyDataEntry entry);
}

class MyHourlyDataEntryHandler implements HourlyDataEntryHandler {

    public Map<String, Record> recordMap = new HashMap<>();

    Record getEmptyRecordForEntry(HourlyDataEntry entry) {
        Record ret = new Record();
        String geohash = Geohash.encode(
                entry.location.latitude,
                entry.location.longitude,
                AirQualityClient.AIR_QUALITY_GEOHASH_LENGTH
        );
        ret.setGeohash(geohash);
        ret.setTimestamp(entry.dateTime.toEpochSecond(ZoneOffset.UTC));
        return ret;
    }

    long count = 0;
    @Override
    public void onHourlyDataEntry(HourlyDataEntry entry) {
//        Record r = recordMap.computeIfAbsent(entry.getKey(), k -> getEmptyRecordForEntry(entry));
        Record r = recordMap.get(entry.getKey());
        if (r == null) {
            r = getEmptyRecordForEntry(entry);
            recordMap.put(entry.getKey(), r);
        }

        r.addFeatureValue(entry.parameterName, entry.measurement);
        count++;
    }

    boolean recordIsCompleted(Record r) {
        for (String featureValue : AirQualityClient.possible) {
            if (!r.getFeatures().containsKey(featureValue)) {
                return false;
            }
        }
        return true;
    }

}