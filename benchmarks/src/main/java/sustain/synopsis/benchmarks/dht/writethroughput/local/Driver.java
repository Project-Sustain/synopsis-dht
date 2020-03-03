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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

public class Driver {
    private static Logger LOGGER = Logger.getLogger(Driver.class);

    private final IngestionRequestDispatcher dispatcher;

    private Driver() throws StorageException {
        dispatcher = new IngestionRequestDispatcher();
    }

    private void start() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        Thread serverT = new Thread(new Runnable() {
            @Override
            public void run() {
                int port = Context.getInstance().getNodeConfig().getIngestionServicePort();
                BindableService[] services = new BindableService[]{new IngestionService(dispatcher)};
                Node node = new Node(port, services);
                node.start(latch);
            }
        });
        serverT.start();
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Error waiting till server to start.", e);
            throw e;
        }
    }

    private void runTestSuite(){
        byte[] payload = new byte[100];
        new Random().nextBytes(payload);
        ByteString serializedStrand = ByteString.copyFrom(payload);
        IngestionRequest request =
                IngestionRequest.newBuilder().setDatasetId("noaa").setEntityId("9xj").setFromTS(1000).setToTS(1100).setSessionId(1).setStrand(serializedStrand).build();
        CompletableFuture<IngestionResponse> future = dispatcher.dispatch(request);
        try {
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
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
            driver.runTestSuite();
        } catch (StorageException | InterruptedException e) {
            LOGGER.error("Error launching the benchmark.", e);
        }
    }
}
