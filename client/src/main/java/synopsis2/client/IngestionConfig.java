package synopsis2.client;

import sustain.synopsis.sketch.dataset.Quantizer;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;

public class IngestionConfig {
    private final Map<String, Quantizer> quantizers;
    private final int temporalBracketLength;
    private final Duration temporalGranularity;

    public IngestionConfig(Map<String, Quantizer> quantizers, int temporalBracketLength, Duration temporalGranularity) {
        this.quantizers = quantizers;
        this.temporalBracketLength = temporalBracketLength;
        this.temporalGranularity = temporalGranularity;
    }

    public Quantizer getQuantizer(String featureName) {
        return quantizers.get(featureName);
    }

    public Map<String, Quantizer> getQuantizers() {
        return quantizers;
    }

    public int getTemporalBracketLength() {
        return temporalBracketLength;
    }

    public Collection<String> getFeatures() {
        return quantizers.keySet();
    }

    public Duration getTemporalGranularity() {
        return temporalGranularity;
    }
}
