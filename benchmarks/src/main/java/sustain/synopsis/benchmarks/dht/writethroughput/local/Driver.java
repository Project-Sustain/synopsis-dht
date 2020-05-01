package sustain.synopsis.benchmarks.dht.writethroughput.local;

import com.google.protobuf.ByteString;
import org.apache.log4j.Logger;
import sustain.synopsis.benchmarks.dht.BenchmarkBaseNode;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.dht.services.ingestion.DHTIngestionRequestProcessor;
import sustain.synopsis.dht.services.ingestion.IngestionRequestProcessor;
import sustain.synopsis.dht.services.ingestion.IngestionService;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.Strand;
import sustain.synopsis.dht.store.services.TerminateSessionRequest;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Driver extends BenchmarkBaseNode {

    private class LoadGenerator implements Runnable {
        private final String[] entities;
        private final Random rnd = new Random(Thread.currentThread().getId());
        private final long requestCount;
        private final int id;

        private LoadGenerator(int id, String[] entities, long requestCount) {
            this.id = id;
            this.entities = entities;
            this.requestCount = requestCount;
        }

        public IngestionRequest generateIngestRequest(long ts) {
            List<Strand> strandBatchList = new ArrayList<>(BATCH_SIZE);
            for (int x = 0; x < BATCH_SIZE; x++) {
                strandBatchList.add(x, Strand.newBuilder().setEntityId(entities[rnd.nextInt(entities.length)])
                                             .setFromTs(ts).setToTs(ts + 1)
                                             .setBytes(payloads.get(rnd.nextInt(PRECALCULATED_PAYLOAD_COUNT))).build());
            }
            return IngestionRequest.newBuilder().setDatasetId("noaa").setSessionId(id).addAllStrand(strandBatchList)
                                   .build();
        }

        @Override
        public void run() {
            try {
                LOGGER.info("Request Count: " + requestCount);
                for (long i = 0; i < requestCount; i++) {
                    // a primitive throttling mechanism
                    if ((submittedReqCount.get() - completedReqCount.get()) > 10000) {
                        Thread.sleep(5);
                        i--;
                        continue;
                    }
                    IngestionRequest request = generateIngestRequest(i);
                    CompletableFuture<IngestionResponse> future = dispatcher.process(request);
                    future.thenAccept(
                            ingestionResponse -> completedReqCount.getAndAdd(ingestionResponse.getStatus() ? 1 : 0));
                    submittedReqCount.getAndIncrement();
                }
                System.out.println("Terminating session..");
                dispatcher.process(TerminateSessionRequest.newBuilder().setDatasetId("noaa").setSessionId(id).build());
            } catch (Throwable e) {
                LOGGER.error("Error in load generator.", e);
            }
        }
    }

    private static Logger LOGGER = Logger.getLogger(Driver.class);
    public static final int PRECALCULATED_PAYLOAD_COUNT = 50000;
    public static final int BATCH_SIZE = 50;
    private IngestionRequestProcessor dispatcher;
    private AtomicLong submittedReqCount = new AtomicLong(0);
    private AtomicLong completedReqCount = new AtomicLong(0);
    private ScheduledExecutorService statThread = Executors.newScheduledThreadPool(1);
    // randomly pick a strand to reduce the effect of compaction
    private final List<ByteString> payloads = generateRandomStrands(PRECALCULATED_PAYLOAD_COUNT, 20);
    private ExecutorService workers = Executors.newCachedThreadPool();
    private long lastReportedTS = -1;
    private long lastReportedCount = -1;

    private Driver(String configFilePath) {
        super(configFilePath);
    }

    private void init() throws StorageException, InterruptedException {
        super.initContext();
        NodeStore nodeStore = new NodeStore();
        nodeStore.init();
        dispatcher = new DHTIngestionRequestProcessor(nodeStore);
        // a thread that reports the write throughout over time
        statThread.scheduleAtFixedRate(() -> {
            if (lastReportedTS == -1 && lastReportedCount == -1) {
                lastReportedTS = System.currentTimeMillis();
                lastReportedCount = completedReqCount.get();
            } else {
                long elapased = System.currentTimeMillis() - lastReportedTS;
                long reqCount = completedReqCount.get() - lastReportedCount;
                LOGGER.info(
                        "Throughput (writes/s): " + reqCount * 1000 / (elapased * 1.0) + ", elapsed: " + elapased + ", "
                        + "req.count: " + reqCount + ", completed: " + completedReqCount.get() + ", submitted: "
                        + submittedReqCount.get() + ", total written (GB): " + (BATCH_SIZE * completedReqCount.get() * payloads.get(0).size())/(1024 * 1024 * 1024D));
                lastReportedCount = completedReqCount.get();
                lastReportedTS = System.currentTimeMillis();
            }
        }, 5, 10, TimeUnit.SECONDS);
        start(new IngestionService(dispatcher));
    }

    private void runTestSuite(int workerCount) {
        List<String> entities = IntStream.range(0, 10).mapToObj(i -> "9x" + i).collect(Collectors.toList());
        // store 128 GB of data
        long requestCount = Math.round(128 * 1024 * 1024 * 1024D)/(payloads.get(0).size() * BATCH_SIZE * workerCount);
        for (int i = 0; i < workerCount; i++) {
            int finalI = i;
            String[] customEntities =
                    entities.stream().map(s -> s + finalI).collect(Collectors.toList()).toArray(new String[5]);
            workers.submit(new LoadGenerator(i, customEntities, requestCount));
            LOGGER.info("Added a load generator.");
        }
    }

    private static List<ByteString> generateRandomStrands(int batchSize, int featureLength) {
        Random random = new Random(12);
        List<ByteString> strands = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            Path path = new Path();
            double[] values = new double[featureLength];
            for (int j = 0; j < featureLength; j++) {
                double v = random.nextDouble();
                path.add(new Feature("feature_" + (i + 1), v));
                values[j] = v;
            }
            RunningStatisticsND runningStats = new RunningStatisticsND(values);
            DataContainer container = new DataContainer(runningStats);
            path.get(path.size() - 1).setData(container);
            // It is okay to use dummy values for geohash and timestamps here.
            // The timestamps in the key are used for indexing
            sustain.synopsis.common.Strand strand1 =
                    new sustain.synopsis.common.Strand("dummy", System.currentTimeMillis(),
                                                       System.currentTimeMillis() + 100, path);
            sustain.synopsis.common.Strand strand2 =
                    new sustain.synopsis.common.Strand("dummy", strand1.getFromTimeStamp(),
                                                       strand1.getToTimestamp() + 100, path);
            strand1.merge(strand2);
            strands.add(CommonUtil.strandToProtoBuff(strand1).toByteString());
        }
        return strands;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            LOGGER.error("Usage: Driver config_file worker_count    ");
            return;
        }
        String configFilePath = args[0];
        int workerCount = Integer.parseInt(args[1]);
        LOGGER.info("Using the configuration: " + configFilePath);
        try {
            Driver driver = new Driver(configFilePath);
            driver.init();
            driver.runTestSuite(workerCount);
        } catch (StorageException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
