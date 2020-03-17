package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.sketch.dataset.Quantizer;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class SessionSchema {
    private final Map<String, Quantizer> quantizers;
    private final int geohashLength;
    private final Duration temporalBracketLength;

    public SessionSchema(Map<String, Quantizer> quantizers, int temporalBracketLength, Duration temporalGranularity) {
        this.quantizers = quantizers;
        this.geohashLength = temporalBracketLength;
        this.temporalBracketLength = temporalGranularity;
    }

    public Quantizer getQuantizer(String featureName) {
        return quantizers.get(featureName);
    }

    public Map<String, Quantizer> getQuantizers() {
        return quantizers;
    }

    public int getGeohashLength() {
        return geohashLength;
    }

    public Set<String> getFeatures() {
        return quantizers.keySet();
    }

    public Duration getTemporalBracketLength() {
        return temporalBracketLength;
    }
}
