package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

public class TestUtil {
    public static Strand createStrand(Path path, String geohash, long ts, long to, double... features) {
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
    }
}
