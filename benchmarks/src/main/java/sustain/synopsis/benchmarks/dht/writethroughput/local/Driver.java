package sustain.synopsis.benchmarks.dht.writethroughput.local;

import com.google.protobuf.ByteString;
import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.*;
import sustain.synopsis.dht.services.IngestionService;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.services.Entity;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class Driver {

    private class LoadGenerator implements Runnable {

        private final String[] entities;

        private LoadGenerator(String[] entities) {
            this.entities = entities;
        }

        @Override
        public void run() {
            byte[] payload = new byte[2048];
            Random rnd = new Random(Thread.currentThread().getId());
            for (int i = 0; i < 200000000; i++) {
                if ((submittedReqCount.get() - completedReqCount.get()) > 1000) { // a primitive throttling mechanism
                    i--;
                    continue;
                }

                int entityBatchSize = 50;
                List<Entity> entityBatchList = new ArrayList<>(entityBatchSize);
                for (int x = 0; x < entityBatchSize; x++) {
                    rnd.nextBytes(payload);
                    ByteString serializedStrand = ByteString.copyFrom(payload);
                    entityBatchList.set(x, Entity.newBuilder()
                            .setEntityId(entities[rnd.nextInt(entities.length)])
                            .setFromTs(x)
                            .setToTs(x+1)
                            .setBytes(serializedStrand)
                            .build()
                    );
                }

                IngestionRequest request =
                        IngestionRequest.newBuilder()
                                .setDatasetId("noaa")
                                .setSessionId(1)
                                .addAllEntity(entityBatchList)
                                .build();

                CompletableFuture<IngestionResponse> future = dispatcher.dispatch(request);
                future.thenAccept(ingestionResponse -> {
                    completedReqCount.getAndAdd(ingestionResponse.getStatus() ? 1 : 0);
                });
                submittedReqCount.getAndIncrement();
            }
        }
    }

    private static Logger LOGGER = Logger.getLogger(Driver.class);

    private final IngestionRequestDispatcher dispatcher;
    private AtomicLong submittedReqCount = new AtomicLong(0);
    private AtomicLong completedReqCount = new AtomicLong(0);
    private ScheduledExecutorService statThread = Executors.newScheduledThreadPool(1);
    private ExecutorService workers = Executors.newCachedThreadPool();
    private long lastReportedTS = -1;
    private long lastReportedCount = -1;

    private Driver() throws StorageException {
        dispatcher = new IngestionRequestDispatcher();
        // a thread that reports the write throughout over time
        statThread.scheduleAtFixedRate(() -> {
            if (lastReportedTS == -1 && lastReportedCount == -1) {
                lastReportedTS = System.currentTimeMillis();
                lastReportedCount = completedReqCount.get();
            } else {
                long elapased = System.currentTimeMillis() - lastReportedTS;
                long reqCount = completedReqCount.get() - lastReportedCount;
                LOGGER.info("Throughput (writes/s): " + reqCount * 1000 / (elapased * 1.0) + ", elapsed: " + elapased + ", " + "req.count: " + reqCount + ", completed: " + completedReqCount.get() + ", submitted: " + submittedReqCount.get());
                lastReportedCount = completedReqCount.get();
                lastReportedTS = System.currentTimeMillis();
            }
        }, 5, 10, TimeUnit.SECONDS);
    }

    /**
     * Launches a DHT node
     *
     * @throws InterruptedException Stat thread is interrupted
     */
    private void start() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Thread serverT = new Thread(() -> {
            int port = Context.getInstance().getNodeConfig().getIngestionServicePort();
            BindableService[] services = new BindableService[]{new IngestionService(dispatcher)};
            Node node = new Node(port, services);
            node.start(latch);
        });
        serverT.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Error waiting till server to start.", e);
            throw e;
        }
    }

    private void runTestSuite(int workerCount) {
        String[] entities = new String[]{"9x0", "9x1", "9x2", "9x3", "9x4"};
        for (int i = 0; i < workerCount; i++) {
            int finalI = i;
            String[] customEntities =
                    Arrays.stream(entities).map(s -> s+ "_" + finalI).collect(Collectors.toList()).toArray(new String[5]);
            workers.submit(new LoadGenerator(customEntities));
            LOGGER.info("Added a load generator.");
        }
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            LOGGER.error("Usage: Driver config_file worker_count    ");
            return;
        }
        String configFilePath = args[0];
        int workerCount = Integer.parseInt(args[1]);
        LOGGER.info("Using the configuration: " + configFilePath);
        // initialize context
        Context ctx = Context.getInstance();
        try {
            ctx.initialize(configFilePath);
        } catch (IOException e) {
            LOGGER.error("Error initializing node config. Config file not found.", e);
            return;
        }
        // set the hostname
        ctx.setProperty(ServerConstants.HOSTNAME, Util.getHostname());
        LOGGER.info("Successfully initialized node context.");
        try {
            Driver driver = new Driver();
            driver.start();
            Thread.sleep(30 * 1000);
            LOGGER.info("Launching the test suite...");
            driver.runTestSuite(workerCount);
        } catch (StorageException | InterruptedException e) {
            LOGGER.error("Error launching the benchmark.", e);
        }
    }
}
