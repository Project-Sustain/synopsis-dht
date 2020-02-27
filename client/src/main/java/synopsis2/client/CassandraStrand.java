package synopsis2.client;

import com.datastax.driver.mapping.annotations.*;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.feature.Feature;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

@Table(keyspace = "synopsis_cassandra", name="strand")
public class CassandraStrand {

    @PartitionKey(0)
    @Column(name="geohash")
    String geohash;

    @PartitionKey(1)
    @Column(name="session_id")
    Long sessionId;

    @ClusteringColumn(0)
    @Column(name="from_ts")
    Long fromTs;

    @ClusteringColumn(1)
    @Column(name="features_key")
    ByteBuffer featuresKey;

    @ClusteringColumn(2)
    @Column(name="bins_key")
    ByteBuffer binsKey;

    @FrozenValue
    @Column(name="summary")
    CassandraSummary summary;

    public static CassandraStrand fromStrandWithConfig(Strand s, CassandraIngestionConfig config) {
        CassandraStrand cs = new CassandraStrand();
        cs.geohash = s.getGeohash();
        cs.sessionId = config.getSessionId();
        cs.fromTs = s.getFromTimeStamp();
        cs.setFeaturesKey(s.getPath().getLabels(), config);
        cs.setBinsKey(s.getPath().getLabels(), config);
        cs.summary = CassandraSummary.fromRunningStatisticsND(
                s.getPath().getTail().getData().statistics);
//        cs.summary = null;
        return cs;
    }

    private void setFeaturesKey(List<Feature> featureList, CassandraIngestionConfig config) {
        BitSet featureBitSet = new BitSet(config.getFeatureCount());
        for (Feature f : featureList) {
            int featureIdx = config.getFeatureIdx(f.getName());
            featureBitSet.set(featureIdx);
        }
        featuresKey = ByteBuffer.wrap(featureBitSet.toByteArray());
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

        binsKey = ByteBuffer.wrap(cur.toByteArray());
    }
//
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