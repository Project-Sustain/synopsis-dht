package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.StrandRegistry;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

public class StreamFlowRecordCallbackHandler implements RecordCallbackHandler {

    final StrandRegistry strandRegistry;
    final SessionSchema sessionSchema;
    final long temporalBracketLengthSeconds;

    public StreamFlowRecordCallbackHandler(StrandRegistry strandRegistry, SessionSchema sessionSchema) {
        this.strandRegistry = strandRegistry;
        this.sessionSchema = sessionSchema;
        this.temporalBracketLengthSeconds = sessionSchema.getTemporalBracketLength().toMinutes()*60;
    }

    @Override
    public boolean onRecordAvailability(Record record) {
        Strand s = constructStrand(record.getGeohash(), record.getTimestamp(), record.getFeatures().get(StreamFlowClient.DISCHARGE_FEATURE));
        strandRegistry.add(s);
        return true;
    }

    @Override
    public void onTermination() {

    }

    private Strand constructStrand(String geohash, long ts, float data) {
        Path path = new Path(1);
        double[] values = new double[1]; // skip time and location
        long[] temporalBracket = new long[]{ts, ts+temporalBracketLengthSeconds};
        int i = 0;

        Quantizer quantizer = sessionSchema.getQuantizer(StreamFlowClient.DISCHARGE_FEATURE);
        Feature feature = new Feature(StreamFlowClient.DISCHARGE_FEATURE, data);
        Feature quantizedVal = quantizer.quantize(feature);
        values[i++] = data;
        path.add(new Feature(StreamFlowClient.DISCHARGE_FEATURE, quantizedVal));

        // create the data container and set as the data of the last vertex
        RunningStatisticsND rsnd = new RunningStatisticsND(values);
        DataContainer container = new DataContainer(rsnd);
        path.get(path.size() - 1).setData(container);
        return new sustain.synopsis.common.Strand(geohash, temporalBracket[0], temporalBracket[1], path);
    }

}
