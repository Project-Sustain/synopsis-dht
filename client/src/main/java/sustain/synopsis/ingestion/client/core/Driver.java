package sustain.synopsis.ingestion.client.core;

import org.apache.log4j.Logger;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.util.Geohash;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Driver implements RecordCallbackHandler {

    private final Logger logger = Logger.getLogger(Driver.class);
    private final int parallelism;
    private final List<ArrayBlockingQueue<Record>> input;
    private final ExecutorService executorService;
    private final StrandPublisher strandPublisher;
    private final Map<String, Quantizer> quantizerMap;
    private final Duration duration;
    private List<IngestionTask> ingestionTasks;

    public Driver(int parallelism, StrandPublisher strandPublisher, Map<String, Quantizer> quantizerMap,
                  Duration temporalBracketSize) {
        this.parallelism = parallelism;
        this.strandPublisher = strandPublisher;
        this.quantizerMap = quantizerMap;
        input = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            input.add(new ArrayBlockingQueue<>(5));
        }
        executorService = Executors.newFixedThreadPool(parallelism);
        this.duration = temporalBracketSize;
        this.ingestionTasks = new ArrayList<>();
    }


    @Override
    public boolean onRecordAvailability(Record record) {
        // load balance the strand generation based on the geohash
        // given that the input domain is relatively smaller, we do not have to use a hashing function here.
        String geohash = record.getGeohash();
        if (geohash.length() > 3) {
            geohash = geohash.substring(0, 3);
        }
        int i = 1;
        for (char c : geohash.toCharArray()) {
            i = i * Geohash.charLookupTable.get(c);
        }
        try {
            input.get(i % parallelism).put(record);
            return true;
        } catch (InterruptedException ignore) {
            logger.error("Interrupted while waiting to enqueue the record.");
        }
        return false;
    }

    @Override
    public void onTermination() {
        for(IngestionTask ingestionTask : ingestionTasks){
            ingestionTask.terminate();
        }
    }

    /**
     * Start the driver. This is a blocking call. This method returns only when
     * all the threads have launched.
     */
    public void start() {
        CountDownLatch startLatch = new CountDownLatch(parallelism);
        for (int i = 0; i < parallelism; i++) {
            IngestionTask ingestionTask = new IngestionTask(new StrandRegistry(strandPublisher), input.get(i), quantizerMap,
                    this.duration, startLatch);
            ingestionTasks.add(ingestionTask);
            executorService.submit(ingestionTask);
        }
        try {
            startLatch.await();
        } catch (InterruptedException ignore) {

        }
        logger.info("Number of ingestion tasks launched: " + parallelism);
    }
}
