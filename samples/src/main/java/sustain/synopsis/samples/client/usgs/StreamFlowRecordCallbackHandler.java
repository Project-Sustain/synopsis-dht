package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.util.Set;

public class StreamFlowRecordCallbackHandler implements RecordCallbackHandler {

    final StrandRegistry strandRegistry;
    final SessionSchema sessionSchema;
    final TemporalQuantizer temporalQuantizer;

    long totalRecordsHandled = 0;

    public StreamFlowRecordCallbackHandler(StrandRegistry strandRegistry, SessionSchema sessionSchema, TemporalQuantizer temporalQuantizer) {
        this.strandRegistry = strandRegistry;
        this.sessionSchema = sessionSchema;
        this.temporalQuantizer = temporalQuantizer;
    }

    @Override
    public boolean onRecordAvailability(Record record) {
        Strand s = constructStrand(record);
        if (s == null) {
            return true;
        }
        strandRegistry.add(s);
        totalRecordsHandled++;
        return true;
    }

    @Override
    public void onTermination() {

    }

    private Strand constructStrand(Record r) {
        Set<String> features = sessionSchema.getFeatures();

        Path path = new Path(features.size());
        double[] values = new double[features.size()]; // skip time and location
        long[] temporalBracket = temporalQuantizer.getTemporalBoundaries(r.getTimestamp());

        int i = 0;
        for (String featureKey : features) {
            Float featureValue = r.getFeatures().get(featureKey);
            if (featureValue == null) {
                // record is missing a feature
                return null;
            }

            Quantizer quantizer = sessionSchema.getQuantizer(featureKey);
            Feature feature = new Feature(featureKey, featureValue);
            Feature quantizedVal = quantizer.quantize(feature);
            values[i++] = featureValue;
            path.add(new Feature(StreamFlowClient.DISCHARGE_FEATURE, quantizedVal));
        }

        // create the data container and set as the data of the last vertex
        RunningStatisticsND rsnd = new RunningStatisticsND(values);
        DataContainer container = new DataContainer(rsnd);
        path.get(path.size() - 1).setData(container);
        return new Strand(r.getGeohash(), temporalBracket[0], temporalBracket[1], path);
    }

}
