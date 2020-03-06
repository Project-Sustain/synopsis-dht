package sustain.synopsis.ingestion.client.core;

import org.apache.log4j.Logger;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class StrandConversionTask implements Runnable {

    private final ArrayBlockingQueue<Record> input;
    private final StrandRegistry registry;
    private final Map<String, Quantizer> quantizers;
    private final Map<String, TemporalQuantizer> temporalQuantizerMap;
    private final Duration duration;
    private AtomicBoolean terminate = new AtomicBoolean(false);
    private final Logger logger = Logger.getLogger(StrandConversionTask.class);
    private final CountDownLatch startupLatch;
    private final CountDownLatch shutdownLatch;

    StrandConversionTask(StrandRegistry registry, ArrayBlockingQueue<Record> input, Map<String, Quantizer> quantizers,
                         Duration duration, CountDownLatch startupLatch, CountDownLatch shutdownLatch) {
        this.input = input;
        this.registry = registry;
        this.quantizers = quantizers;
        this.temporalQuantizerMap = new HashMap<>();
        this.duration = duration;
        this.startupLatch = startupLatch;
        this.shutdownLatch = shutdownLatch;
    }

    @Override
    public void run() {
        startupLatch.countDown();
        logger.info("[Thread id: " + Thread.currentThread().getName() + "] Starting the ingestion task.");
        while (!(terminate.get() && input.isEmpty())) {
            try {
                Record record = input.poll(5, TimeUnit.SECONDS);
                if (record == null) {
                    continue;
                }
                Strand strand = convertToStrand(record);
                registry.add(strand);
            } catch (InterruptedException ignore) {

            } catch (Throwable e){
                logger.error("Error during processing a record.", e);
            }
        }
        registry.terminateSession();
        logger.info("[Thread id: " + Thread.currentThread().getName() + "] Terminating the ingestion task.");
        shutdownLatch.countDown();
    }

    Strand convertToStrand(Record record) {
        Map<String, Float> features = record.getFeatures();
        // we keep only the feature data in the path. spatio-temporal data is already stored in the path
        Path path = new Path(features.size());
        // need to sort the feature names alphabetically to support merging
        List<String> featureNames = new ArrayList<>(features.keySet());
        Collections.sort(featureNames);
        double[] values = new double[features.size()]; // skip time and location
        int valueIndex = 0;
        for (String featureName : featureNames) {
            Quantizer quantizer = quantizers.get(featureName);
            assert quantizer != null;
            Float featureValue = features.get(featureName);
            Feature quantizedValue = quantizer.quantize(new Feature(featureValue));
            values[valueIndex++] = featureValue;
            path.add(new Feature(featureName, quantizedValue));
        }
        // create the data container and set as the data of the last vertex
        RunningStatisticsND rsnd = new RunningStatisticsND(values);
        DataContainer container = new DataContainer(rsnd);
        path.get(path.size() - 1).setData(container);

        TemporalQuantizer temporalQuantizer;
        if(!temporalQuantizerMap.containsKey(record.getGeohash())){
            temporalQuantizer = new TemporalQuantizer(duration);
            temporalQuantizerMap.put(record.getGeohash(), temporalQuantizer);
        } else {
            temporalQuantizer = temporalQuantizerMap.get(record.getGeohash());
        }
        long[] boundaries = temporalQuantizer.getTemporalBoundaries(record.getTimestamp());
        return new Strand(record.getGeohash(), boundaries[0], boundaries[1], path);
    }

    void terminate() {
        logger.info("[Thread id: " + Thread.currentThread().getName() + "] Terminate request is received.");
        terminate.set(true);
    }
}
