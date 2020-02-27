package synopsis2.client;

import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.dataset.feature.FeatureType;

import java.util.HashMap;

public class CassandraIngestionConfig {

    private HashMap<String, Integer> featureIdxMap;
    private HashMap<String, BinMap> featureBinMap;
    private Long sessionId;

    public static CassandraIngestionConfig fromIngestConfigWithSessionId(IngestionConfig config, long sessionId) {
        CassandraIngestionConfig ret = new CassandraIngestionConfig();

        int idx = 0;
        ret.featureIdxMap = new HashMap<>(config.getFeatures().size());
        ret.featureBinMap = new HashMap<>(config.getFeatures().size());
        for (String fName : config.getFeatures()) {
            Quantizer q = config.getQuantizer(fName);
            ret.featureBinMap.put(fName, BinMap.fromQuantizer(q));
            ret.featureIdxMap.put(fName, idx++);
        }

        ret.sessionId = sessionId;
        return ret;
    }

    public Long getSessionId() {
        return sessionId;
    }

    public Integer getFeatureCount() {
        return featureIdxMap.size();
    }

    public Integer getFeatureBin(Feature f) {
        BinMap bm = featureBinMap.get(f.getName());
        return bm.getBinIdx(f);
    }

    public Integer getFeatureIdx(String featureName) {
        return featureIdxMap.get(featureName);
    }

    public Integer getFeatureBinCount(String featureName) {
        return featureBinMap.get(featureName).getNumBins();
    }

    private static class BinMap {

        private HashMap<Object,Integer> binIdxMap;

        public static BinMap fromQuantizer(Quantizer q) {
            BinMap bm = new BinMap();
            bm.binIdxMap = new HashMap<>(q.numTicks());
            Feature curFeature = q.first();
            for (int i = 0; i < q.numTicks(); i++) {
                Object featureVal = getFeatureValue(curFeature);
                bm.binIdxMap.put(featureVal,i);
                curFeature = q.nextTick(curFeature);
            }
            return bm;
        }

        private static Object getFeatureValue(Feature f) {
            FeatureType type = f.getType();
            switch (type) {
                case NULL:      return new Object();
                case INT:       return f.getInt();
                case LONG:      return f.getLong();
                case FLOAT:     return f.getFloat();
                case DOUBLE:    return f.getDouble();
                case STRING:    return f.getString();
                // TODO this wont work.
                case BINARY:    return new Object();
                default:        return null;
            }
        }

        public int getNumBins() {
            return binIdxMap.size();
        }

        public int getBinIdx(Feature f) {
            Object featureValue = getFeatureValue(f);
            return binIdxMap.get(featureValue);
        }

    }

}
