package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.sketch.dataset.AutoQuantizer;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.dataset.feature.FeatureType;
import sustain.synopsis.sketch.stat.SquaredError;

import java.util.*;

/**
 * Calculates the bin configuration for a given feature based on a sample.
 * Objective is to generate a bin configuration with the minimum number of ticks such that the
 * discretization error is below the given threshold. Priority is given for KDE based bin generation.
 * If it fails to generate a bin configuration within the provided boundaries for the bin count with the
 * provided discretization error boundaries, it is attempted to generate a bin configuration with equal bin
 * widths.
 */
public class BinCalculator {

    public enum BinConfigTypePreference {
        OKDE,
        EVEN_WIDTH,
        ANY
    }

    public enum BinConfigSelectionPreference {
        MIN_BINS,
        MIN_ERROR
    }

    final int minTicks;
    final int maxTicks;
    final double discErrorThreshold;
    final BinConfigTypePreference typePreference;
    final BinConfigSelectionPreference selectionPreference;

    private BinCalculator(int minTicks, int maxTicks, double discErrorThreshold, BinConfigTypePreference typePreference, BinConfigSelectionPreference selectionPreference) {
        this.minTicks = minTicks;
        this.maxTicks = maxTicks;
        this.discErrorThreshold = discErrorThreshold;
        this.typePreference = typePreference;
        this.selectionPreference = selectionPreference;
    }

    public final static BinCalculator DEFAULT_BIN_CALCULATOR = new BinCalculator(5, 50, 0.025, BinConfigTypePreference.ANY, BinConfigSelectionPreference.MIN_BINS);

    public static BinCalculatorBuilder newBuilder() {
        return new BinCalculatorBuilder();
    }

    public static class BinCalculatorBuilder {
        int minTicks = 5;
        int maxTicks = 50;
        double discErrorThreshold = 0.025;
        BinConfigTypePreference typePreference = BinConfigTypePreference.ANY;
        BinConfigSelectionPreference selectionPreference = BinConfigSelectionPreference.MIN_BINS;

        public BinCalculatorBuilder setMinTicks(int minTicks) {
            this.minTicks = minTicks;
            return this;
        }

        public BinCalculatorBuilder setMaxTicks(int maxTicks) {
            this.maxTicks = maxTicks;
            return this;
        }

        public BinCalculatorBuilder setDiscErrorThreshold(double discErrorThreshold) {
            this.discErrorThreshold = discErrorThreshold;
            return this;
        }

        public BinCalculatorBuilder setTypePreference(BinConfigTypePreference typePreference) {
            this.typePreference = typePreference;
            return this;
        }

        public BinCalculatorBuilder setSelectionPreference(BinConfigSelectionPreference selectionPreference) {
            this.selectionPreference = selectionPreference;
            return this;
        }

        public BinCalculator build() {
            return new BinCalculator(minTicks, maxTicks, discErrorThreshold, typePreference, selectionPreference);
        }
    }

    public enum BinConfigType {
        OKDE,
        EVEN_WIDTH
    };

    public static class BinResult {
        final String featureName;
        final BinConfigType type;
        final double rmse;
        final List<Feature> sample;
        final Quantizer quantizer;

        public BinResult(String featureName, BinConfigType type, double rmse, List<Feature> sample, Quantizer quantizer) {
            this.featureName = featureName;
            this.type = type;
            this.rmse = rmse;
            this.sample = sample;
            this.quantizer = quantizer;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(featureName);
            for (Feature f : quantizer.getTicks()) {
                sb.append(","+f.getFloat());
            }
            return sb.toString();
        }

        public String getFeatureName() {
            return featureName;
        }

        public BinConfigType getType() {
            return type;
        }

        public double getRmse() {
            return rmse;
        }

        public List<Feature> getSample() {
            return sample;
        }

        public Quantizer getQuantizer() {
            return quantizer;
        }

    }

    public static class BinCalculatorResult {
        final List<BinResult> binResults;

        public BinCalculatorResult(List<BinResult> binResults) {
            this.binResults = new ArrayList<>(binResults);
            this.binResults.sort(Comparator.comparing(BinResult::getFeatureName));
        }

        public BinResult getBinResultForFeature(String featureName) {
            for (BinResult b : binResults) {
                if (b.featureName.equals(featureName)) {
                    return b;
                }
            }
            return null;
        }

        public List<BinResult> getBinResults() {
            return binResults;
        }

        public double getMaxRmse() {
            double max = 0;
            for (BinResult binResult : binResults) {
                if (binResult.getRmse() > max) {
                    max = binResult.getRmse();
                }
            }
            return max;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < binResults.size()-1; i++) {
                sb.append(binResults.get(i));
                sb.append("\t");
            }
            if (binResults.size() > 0) {
                sb.append(binResults.get(binResults.size()-1));
            }
            return sb.toString();
        }

    }

    public BinCalculatorResult calculateBins(List<Record> recordList) {
        Map<String, List<Feature>> featureMap = featureMapFromRecords(recordList);
        List<BinResult> binResults = new ArrayList<>();
        for (Map.Entry<String, List<Feature>> entry : featureMap.entrySet()) {

            BinResult res = null;
            switch (selectionPreference) {
                case MIN_BINS:
                    res = calculateFirstUnderThreshold(entry.getValue(), entry.getKey());
                case MIN_ERROR:
                    res = calculateLowestError(entry.getValue(), entry.getKey());
            }

            if (res == null) {
                return null;
            }
            binResults.add(res);
        }
        return new BinCalculatorResult(binResults);
    }

    BinResult calculateFirstUnderThreshold(List<Feature> sample, String featureName) {
        for (int ticks = minTicks; ticks <= maxTicks; ticks++) {
            BinResult res;
            switch (typePreference) {
                case OKDE:
                    res = calculateOkdeBinConfig(sample, featureName, ticks);
                    break;
                case EVEN_WIDTH:
                    res = calculateEvenWidthBinConfig(sample, featureName, ticks);
                    break;
                case ANY:
                default:
                    res = calculateBestBinConfig(sample, featureName, ticks);
            }
            if (res.getRmse() < discErrorThreshold) {
                return res;
            }
        }
        return null;
    }

    BinResult calculateLowestError(List<Feature> sample, String featureName) {
        BinResult best = null;
        for (int ticks = minTicks; ticks <= maxTicks; ticks++) {
            BinResult res;
            switch (typePreference) {
                case OKDE:
                    res = calculateOkdeBinConfig(sample, featureName, ticks);
                    break;
                case EVEN_WIDTH:
                    res = calculateEvenWidthBinConfig(sample, featureName, ticks);
                    break;
                case ANY:
                default:
                    res = calculateBestBinConfig(sample, featureName, ticks);

            }
            if (res.getRmse() < discErrorThreshold && (best == null || res.getRmse() < best.getRmse())) {
                best = res;
            }
        }
        return best;
    }

    public static BinResult calculateBestBinConfig(List<Feature> sample, String featureName, int binCount) {
        BinResult okde = calculateOkdeBinConfig(sample, featureName, binCount);
        BinResult even = calculateEvenWidthBinConfig(sample, featureName, binCount);
        if (okde != null && okde.getRmse() < even.getRmse()) {
            return okde;
        } else {
            return even;
        }
    }

    public static BinResult calculateOkdeBinConfig(List<Feature> sample, String featureName, int binCount) {
        try {
            Quantizer quantizer = AutoQuantizer.fromList(sample, binCount);
            double rmse = evaluate(sample, quantizer);
            return new BinResult(featureName, BinConfigType.OKDE, rmse, sample, quantizer);
        } catch (Exception e) {
            return null;
        }
    }

    public static BinResult calculateEvenWidthBinConfig(List<Feature> sample, String featureName, int binCount) {
        double min = sample.get(0).getDouble();
        double max = sample.get(sample.size()-1).getDouble();
        Quantizer quantizer = calculateGlobalEvenQuantizer(min, max, binCount);
        double rmse = evaluate(sample, quantizer);
        return new BinResult(featureName, BinConfigType.EVEN_WIDTH, rmse, sample, quantizer);
    }

    public static Map<String, List<Feature>> featureMapFromRecords(List<Record> recordList) {
        Map<String,List<Feature>> featureListMap = new HashMap<>();
        for (Record r : recordList) {
            Map<String, Float> features = r.getFeatures();
            for (String featureName : features.keySet()) {
                featureListMap.computeIfAbsent(featureName, k -> new ArrayList<>())
                        .add(new Feature(featureName, features.get(featureName)));
            }
        }

        for (List<Feature> list : featureListMap.values()) {
            list.sort(Comparator.comparing(Feature::getDouble));
        }
        return featureListMap;
    }

    static Quantizer calculateGlobalEvenQuantizer(double min, double max, int tickCount) {
        double range = max - min;
        double stepSize = range / (tickCount - 1);
        List<Feature> features = new ArrayList<>(tickCount);
        for (int i = 0; i < tickCount; i++) {
            features.add(new Feature(min + i * stepSize));
        }
        return new Quantizer(features);
    }

    static double evaluate(List<Feature> sample, Quantizer q) {
        List<Feature> quantized = new ArrayList<>();
        for (Feature f : sample) {
            /* Find the midpoint */
            Feature initial = q.quantize(f.convertTo(FeatureType.DOUBLE));
            Feature next = q.nextTick(initial);
            if (next == null) {
                next = initial;
            }
            Feature difference = next.subtract(initial);
            Feature midpoint = difference.divide(new Feature(2.0f));
            Feature prediction = initial.add(midpoint);

            quantized.add(prediction);
        }
        SquaredError se = new SquaredError(sample, quantized);
        //System.out.println(se.RMSE() + "," + se.NRMSE() + "," + se.CVRMSE());
        return se.NRMSE();
    }

}
