package synopsis2.client;

import sustain.synopsis.sketch.dataset.Quantizer;

import java.time.Duration;
import java.util.List;
import java.util.Map;

public class IngestionConfig {
    private final List<String> features;
    private final Map<String, Quantizer> quantizers;
    private final int precision;
    private final Duration temporalGranularity;

    public IngestionConfig(List<String> features, Map<String, Quantizer> quantizers, int precision, Duration temporalGranularity) {
        this.features = features;
        this.quantizers = quantizers;
        this.precision = precision;
        this.temporalGranularity = temporalGranularity;
    }

    public Quantizer getQuantizer(String featureName) {
        return quantizers.get(featureName);
    }

    public int getPrecision() {
        return precision;
    }

    public List<String> getFeatures() {
        return features;
    }

    public Duration getTemporalGranularity() {
        return temporalGranularity;
    }
}
