/*
 *
 * Software in the Sustain Ecosystem are Released Under Terms of Apache Software License 
 *
 * This research has been supported by funding from the US National Science Foundation's CSSI program through awards 1931363, 1931324, 1931335, and 1931283. The project is a joint effort involving Colorado State University, Arizona State University, the University of California-Irvine, and the University of Maryland - Baltimore County. All redistributions of the software must also include this information. 
 *
 * TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
 *
 *
 * 1. Definitions.
 *
 * "License" shall mean the terms and conditions for use, reproduction, and distribution as defined by Sections 1 through 9 of this document.
 *
 * "Licensor" shall mean the copyright owner or entity authorized by the copyright owner that is granting the License.
 *
 * "Legal Entity" shall mean the union of the acting entity and all other entities that control, are controlled by, or are under common control with that entity. For the purposes of this definition, "control" means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.
 *
 * "You" (or "Your") shall mean an individual or Legal Entity exercising permissions granted by this License.
 *
 * "Source" form shall mean the preferred form for making modifications, including but not limited to software source code, documentation source, and configuration files.
 *
 * "Object" form shall mean any form resulting from mechanical transformation or translation of a Source form, including but not limited to compiled object code, generated documentation, and conversions to other media types.
 *
 * "Work" shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work (an example is provided in the Appendix below).
 *
 * "Derivative Works" shall mean any work, whether in Source or Object form, that is based on (or derived from) the Work and for which the editorial revisions, annotations, elaborations, or other modifications represent, as a whole, an original work of authorship. For the purposes of this License, Derivative Works shall not include works that remain separable from, or merely link (or bind by name) to the interfaces of, the Work and Derivative Works thereof.
 *
 * "Contribution" shall mean any work of authorship, including the original version of the Work and any modifications or additions to that Work or Derivative Works thereof, that is intentionally submitted to Licensor for inclusion in the Work by the copyright owner or by an individual or Legal Entity authorized to submit on behalf of the copyright owner. For the purposes of this definition, "submitted" means any form of electronic, verbal, or written communication sent to the Licensor or its representatives, including but not limited to communication on electronic mailing lists, source code control systems, and issue tracking systems that are managed by, or on behalf of, the Licensor for the purpose of discussing and improving the Work, but excluding communication that is conspicuously marked or otherwise designated in writing by the copyright owner as "Not a Contribution."
 *
 * "Contributor" shall mean Licensor and any individual or Legal Entity on behalf of whom a Contribution has been received by Licensor and subsequently incorporated within the Work.
 *
 * 2. Grant of Copyright License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare Derivative Works of, publicly display, publicly perform, sublicense, and distribute the Work and such Derivative Works in Source or Object form.
 *
 * 3. Grant of Patent License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such license applies only to those patent claims licensable by such Contributor that are necessarily infringed by their Contribution(s) alone or by combination of their Contribution(s) with the Work to which such Contribution(s) was submitted. If You institute patent litigation against any entity (including a cross-claim or counterclaim in a lawsuit) alleging that the Work or a Contribution incorporated within the Work constitutes direct or contributory patent infringement, then any patent licenses granted to You under this License for that Work shall terminate as of the date such litigation is filed.
 *
 * 4. Redistribution. You may reproduce and distribute copies of the Work or Derivative Works thereof in any medium, with or without modifications, and in Source or Object form, provided that You meet the following conditions:
 *
 * You must give any other recipients of the Work or Derivative Works a copy of this License; and
 * You must cause any modified files to carry prominent notices stating that You changed the files; and
 * You must retain, in the Source form of any Derivative Works that You distribute, all copyright, patent, trademark, and attribution notices from the Source form of the Work, excluding those notices that do not pertain to any part of the Derivative Works; and
 * If the Work includes a "NOTICE" text file as part of its distribution, then any Derivative Works that You distribute must include a readable copy of the attribution notices contained within such NOTICE file, excluding those notices that do not pertain to any part of the Derivative Works, in at least one of the following places: within a NOTICE text file distributed as part of the Derivative Works; within the Source form or documentation, if provided along with the Derivative Works; or, within a display generated by the Derivative Works, if and wherever such third-party notices normally appear. The contents of the NOTICE file are for informational purposes only and do not modify the License. You may add Your own attribution notices within Derivative Works that You distribute, alongside or as an addendum to the NOTICE text from the Work, provided that such additional attribution notices cannot be construed as modifying the License. 
 *
 * You may add Your own copyright statement to Your modifications and may provide additional or different license terms and conditions for use, reproduction, or distribution of Your modifications, or for any such Derivative Works as a whole, provided Your use, reproduction, and distribution of the Work otherwise complies with the conditions stated in this License.
 * 5. Submission of Contributions. Unless You explicitly state otherwise, any Contribution intentionally submitted for inclusion in the Work by You to the Licensor shall be under the terms and conditions of this License, without any additional terms or conditions. Notwithstanding the above, nothing herein shall supersede or modify the terms of any separate license agreement you may have executed with Licensor regarding such Contributions.
 *
 * 6. Trademarks. This License does not grant permission to use the trade names, trademarks, service marks, or product names of the Licensor, except as required for reasonable and customary use in describing the origin of the Work and reproducing the content of the NOTICE file.
 *
 * 7. Disclaimer of Warranty. Unless required by applicable law or agreed to in writing, Licensor provides the Work (and each Contributor provides its Contributions) on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of permissions under this License.
 *
 * 8. Limitation of Liability. In no event and under no legal theory, whether in tort (including negligence), contract, or otherwise, unless required by applicable law (such as deliberate and grossly negligent acts) or agreed to in writing, shall any Contributor be liable to You for damages, including any direct, indirect, special, incidental, or consequential damages of any character arising as a result of this License or out of the use or inability to use the Work (including but not limited to damages for loss of goodwill, work stoppage, computer failure or malfunction, or any and all other commercial damages or losses), even if such Contributor has been advised of the possibility of such damages.
 *
 * 9. Accepting Warranty or Additional Liability. While redistributing the Work or Derivative Works thereof, You may choose to offer, and charge a fee for, acceptance of support, warranty, indemnity, or other liability obligations and/or rights consistent with this License. However, in accepting such obligations, You may act only on Your own behalf and on Your sole responsibility, not on behalf of any other Contributor, and only if You agree to indemnify, defend, and hold each Contributor harmless for any liability incurred by, or claims asserted against, such Contributor by reason of your accepting any such warranty or additional liability. 
 *
 * END OF TERMS AND CONDITIONS */
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
