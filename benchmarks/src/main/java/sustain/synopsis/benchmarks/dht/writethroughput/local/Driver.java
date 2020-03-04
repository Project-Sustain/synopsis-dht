package sustain.synopsis.benchmarks.dht.writethroughput.local;

import com.google.protobuf.ByteString;
import io.grpc.BindableService;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.*;
import sustain.synopsis.dht.services.IngestionService;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class Driver {
    private static Logger LOGGER = Logger.getLogger(Driver.class);

    private final IngestionRequestDispatcher dispatcher;
    private AtomicLong submittedReqCount = new AtomicLong(0);
    private AtomicLong completedReqCount = new AtomicLong(0);
    private ScheduledExecutorService statThread = Executors.newScheduledThreadPool(1);
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
     * @throws InterruptedException
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

    private void runTestSuite() {
        byte[] payload = new byte[100];
        String[] entities = new String[]{"9x0", "9x1", "9x2", "9x3", "9x4", "9x5", "9x6", "9x7", "9x8", "9x9"};
        Random rnd = new Random(1);
        for (int i = 0; i < 200000000; i++) {
            if ((submittedReqCount.get() - completedReqCount.get()) > 10000) { // a primitive throttling mechanism
                i--;
                continue;
            }
            rnd.nextBytes(payload);
            ByteString serializedStrand = ByteString.copyFrom(payload);
            IngestionRequest request =
                IngestionRequest.newBuilder().setDatasetId("noaa").setEntityId(entities[rnd.nextInt(entities.length)]).setFromTS(i).setToTS(i + 1).setSessionId(1).setStrand(serializedStrand).build();
            CompletableFuture<IngestionResponse> future = dispatcher.dispatch(request);
            future.thenAccept(ingestionResponse -> {
                completedReqCount.getAndAdd(ingestionResponse.getStatus() ? 1 : 0);
            });
            submittedReqCount.getAndIncrement();
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            LOGGER.error("Path to the configuration file is missing. Exiting!");
            return;
        }
        String configFilePath = args[0];
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
            driver.runTestSuite();
        } catch (StorageException | InterruptedException e) {
            LOGGER.error("Error launching the benchmark.", e);
        }
    }
}
