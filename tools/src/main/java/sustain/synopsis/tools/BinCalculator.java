package sustain.synopsis.tools;

import sustain.synopsis.sketch.dataset.AutoQuantizer;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.dataset.feature.FeatureType;
import sustain.synopsis.sketch.stat.SquaredError;

import java.util.ArrayList;
import java.util.List;

/**
 * Calculates the bin configuration for a given feature based on a sample.
 * Objective is to generate a bin configuration with the minimum number of ticks such that the
 * discretization error is below the given threshold. Priority is given for KDE based bin generation.
 * If it fails to generate a bin configuration within the provided boundaries for the bin count with the
 * provided discretization error boundaries, it is attempted to generate a bin configuration with equal bin
 * widths.
 */
public class BinCalculator {
    /**
     * Generate a bin configuration within the given constraints on tick count and the discretization error.
     * Starts with the minimum number of ticks and generates a bin configuration. Then the discretization error of
     * the bin configuration is calculated w.r.t. to the sample. If it is higher than the given threshold, increments
     * the number of ticks and repeats the same process.
     * First it is attempted to generate a bin config. using the oKDE. If it fails to produce a bin config. after
     * reaching the maximum tick count, switch to even-width bin calculation.
     *
     * @param sample             Sample of the feature values. Minimum - 1000 elements
     * @param discErrorThreshold Discretization error threshold as NRMSE. The value should be less than < 1.0
     * @param minTicks           Minimum number of ticks allowed in a bin config.
     * @param maxTicks           Maximum number of ticks allowed in a bin config.
     * @return List of ticks comprises the bin configuration. It may return <code>null</code> if it fails to
     * generate a bin config within the given constraints
     */
    public List<Feature> calculateBins(List<Feature> sample, double discErrorThreshold, int minTicks, int maxTicks) {
        if (sample.size() < 1000) {
            throw new IllegalArgumentException("Sample size should be atleast 1000 elements.");
        }
        if (discErrorThreshold >= 1.0) {
            throw new IllegalArgumentException("Discretization error should be less than 1.0");
        }
        if (minTicks <= 0) {
            throw new IllegalArgumentException("Min tick count should be greater than 0");
        }
        if (minTicks >= maxTicks) {
            throw new IllegalArgumentException("Min tick count should be less than the max tick count.");

        }
        Quantizer quantizer = null;
        // try using oKDE based quantizer generation first
        for (int tickCount = minTicks; tickCount <= maxTicks; tickCount++) {
            try {
                quantizer = AutoQuantizer.fromList(sample, tickCount);
                double discError = evaluate(sample, quantizer);
                System.out.println("Algorithm: oKDE, tick count: " + tickCount + ", discretization error: " + discError);
                if (discError <= discErrorThreshold) {
                    System.out.println("Found a bin configuration.");
                    return quantizer.getTicks();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("oKDE failed to produce a bin config. Switching to even width bin configuration.");
        // oKDE based quantizer generation has not produced a quantizer. Switching to even spaced quantizer generation
        List<Feature> oKDETicks = quantizer.getTicks();
        double min = oKDETicks.get(0).getDouble();
        double max = oKDETicks.get(oKDETicks.size() - 1).getDouble();
        for (int tickCount = minTicks; tickCount <= maxTicks; tickCount++) {
            Quantizer evenIntervalQuantizer = calculateGlobalEvenQuantizer(min, max, tickCount);
            double discError = evaluate(sample, evenIntervalQuantizer);
            System.out.println("Algorithm: even-width, tick count: " + tickCount + ", discretization error: " + discError);
            if (discError <= discErrorThreshold) {
                System.out.println("Found a bin configuration.");
                return evenIntervalQuantizer.getTicks();
            }
        }
        System.err.println("Failed to produce a bin configuration within the given constraints.");
        return null;
    }

    /**
     * Overloaded version of the previous method with commonly used defaults.
     * @param sample Sample of feature values.
     * @return List of ticks comprising a bin configuration
     */
    public List<Feature> calculateBins(List<Feature> sample){
        return calculateBins(sample, 0.025, 5, 50);
    }

    private double evaluate(List<Feature> sample, Quantizer q) {
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

    private Quantizer calculateGlobalEvenQuantizer(double min, double max, int tickCount) {
        double range = max - min;
        double stepSize = range / (tickCount - 1);
        List<Feature> features = new ArrayList<>(tickCount);
        for (int i = 0; i < tickCount; i++) {
            features.add(new Feature(min + i * stepSize));
        }
        return new Quantizer(features);
    }
}
