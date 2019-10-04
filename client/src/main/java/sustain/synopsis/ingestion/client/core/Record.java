package sustain.synopsis.ingestion.client.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Represents a single spatio-temporal record that is being sketched.
 * {@link DataConnector}  generates records and passes to the {@link Driver}
 */
public class Record {
    private float latitude;
    private float longitude;
    /**
     * Epoch time (in UTC)
     */
    private long timestamp;
    private Properties metadata;
    private Map<String, Float> features;

    public Record() {
        metadata = new Properties();
        features = new HashMap<>();
    }

    public void setLatitude(float latitude) {
        this.latitude = latitude;
    }

    public void setLongitude(float longitude) {
        this.longitude = longitude;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    private void addMetadataProperty(String propName, String propValue){
        metadata.setProperty(propName, propValue);
    }

    private void addFeatureValue(String featureName, float featureValue){
        features.put(featureName, featureValue);
    }
}
