package synopsis2.client;

import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.math.BigInteger;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

public class CassandraStrand {
    String geohash;
    Long sessionId;
    Long fromTs;
    byte[] featuresKey;
    byte[] binsKey;
    RunningStatisticsND statistics;

    public static CassandraStrand fromStrandWithConfig(Strand s, CassandraIngestionConfig config) {
        CassandraStrand cs = new CassandraStrand();
        cs.geohash = s.getGeohash();
        cs.sessionId = config.getSessionId();
        cs.fromTs = s.getFromTimeStamp();
        cs.setFeaturesKey(s.getPath().getLabels(), config);
        cs.setBinsKey(s.getPath().getLabels(), config);
        cs.statistics = s.getPath().getTail().getData().statistics;
        return cs;
    }

    private void setFeaturesKey(List<Feature> featureList, CassandraIngestionConfig config) {
        BitSet featureBitSet = new BitSet(config.getFeatureCount());
        for (Feature f : featureList) {
            int featureIdx = config.getFeatureIdx(f.getName());
            featureBitSet.set(featureIdx);
        }
        featuresKey = featureBitSet.toByteArray();
    }

    private void setBinsKey(List<Feature> featureList, CassandraIngestionConfig config) {
        featureList.sort(new OrdinalFeatureComparator(config));

        BigInteger cur = BigInteger.ONE;
        for (Feature f : featureList) {
            int base = config.getFeatureBinCount(f.getName());
            int value = config.getFeatureBin(f);

            cur = cur.multiply(BigInteger.valueOf(base))
                    .add(BigInteger.valueOf(value));
        }

        binsKey = cur.toByteArray();
    }

//        private static String getPathKey(Path p) {
//            StringBuilder sb = new StringBuilder();
//            for (Feature f : p.getLabels()) {
//                sb.append(f.getName());
//            }
//            return sb.toString();
//        }

}

class OrdinalFeatureComparator implements Comparator<Feature> {

    CassandraIngestionConfig config;

    public OrdinalFeatureComparator(CassandraIngestionConfig config) {
        this.config = config;
    }

    @Override
    public int compare(Feature f1, Feature f2) {
        Integer f1Idx = config.getFeatureIdx(f1.getName());
        Integer f2Idx = config.getFeatureIdx(f2.getName());
        return f1Idx.compareTo(f2Idx);
    }

}