package sustain.synopsis.ingestion.client.core;

import java.util.HashMap;
import java.util.Map;

/**
 * Represents a single spatio-temporal record that is being sketched.
 * {@link DataConnector}  generates records and passes to the {@link Driver}
 */
public class Record {
    private String geohash;
    /**
     * Epoch time (in UTC)
     */
    private long timestamp;
    private final Map<String, Double> features;

    public Record() {
        features = new HashMap<>();
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public void addFeatureValue(String featureName, double featureValue){
        features.put(featureName, featureValue);
    }

    public void setGeohash(String geohash) {
        this.geohash = geohash;
    }

    public String getGeohash() {
        return geohash;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Map<String, Double> getFeatures() {
        return features;
    }
}
