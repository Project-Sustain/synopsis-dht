package synopsis2.client;

import io.sigpipe.sing.dataset.analysis.Quantizer;

import java.util.List;
import java.util.Map;

public class IngestionConfig {
    private final List<String> features;
    private final Map<String, Quantizer> quantizers;
    private final int precision;

    public IngestionConfig(List<String> features, Map<String, Quantizer> quantizers, int precision) {
        this.features = features;
        this.quantizers = quantizers;
        this.precision = precision;
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
}