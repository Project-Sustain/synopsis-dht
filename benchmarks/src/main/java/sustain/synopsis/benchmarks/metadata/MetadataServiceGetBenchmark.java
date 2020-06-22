package sustain.synopsis.benchmarks.metadata;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.metadata.*;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class MetadataServiceGetBenchmark {

    static MetadataServiceGrpc.MetadataServiceFutureStub stub;
    static ExecutorService executorService = Executors.newSingleThreadExecutor();
    static long requestsFinished;

    static GetMetadataRequest getRequest(int requestSize) {
        GetMetadataRequest.Builder requestBuilder = GetMetadataRequest.newBuilder();
        int myRequestSize = 0;
        for (int datasetId = 0; myRequestSize < requestSize; datasetId++) {
            DatasetSessions.Builder sessionsBuilder = DatasetSessions.newBuilder()
                    .setDatasetId(String.valueOf(datasetId));

            for (int sessionId = 0; sessionId < 100 && myRequestSize < requestSize; sessionId++) {
                sessionsBuilder.addSessionId(sessionId);
                myRequestSize++;
            }
            requestBuilder.addDatasetSessions(sessionsBuilder.build());
        }
        return requestBuilder.build();
    }

    static double runBenchmark(int binConfigsPerRequest, int maxConcurrentRequests, int benchmarkDurationSeconds) throws InterruptedException {
        GetMetadataRequest request = getRequest(binConfigsPerRequest);
        requestsFinished = 0;

        Semaphore semaphore = new Semaphore(maxConcurrentRequests);
        long endTime = System.currentTimeMillis() + benchmarkDurationSeconds * 1000;
        while (System.currentTimeMillis() < endTime) {
            semaphore.acquire();
            if (System.currentTimeMillis() >= endTime) {
                break;
            }
            ListenableFuture<GetMetadataResponse> responseListenableFuture = stub.getMetadata(request);
            responseListenableFuture.addListener(() -> {
                try {
                    GetMetadataResponse resp = responseListenableFuture.get();
                    resp.getDatasetMetadataCount();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                requestsFinished++;
                semaphore.release();
            }, executorService);
        }

        double msgPerSec = (double)requestsFinished/ benchmarkDurationSeconds;
        double binConfigsPerSecond = (double)(requestsFinished*binConfigsPerRequest) / benchmarkDurationSeconds;
        return binConfigsPerSecond;
    }

    public static void main(String[] args) throws InterruptedException {
        String metadataHost = args[0];
        int metadataPort = Integer.parseInt(args[1]);
        int maxConcurrentRequests = Integer.parseInt(args[2]);
        int binConfigsPerRequest = Integer.parseInt(args[3]);
        int benchmarkDurationSeconds = Integer.parseInt(args[4]);

        Channel channel = ManagedChannelBuilder.forAddress(metadataHost, metadataPort)
                .usePlaintext()
                .build();
        stub = MetadataServiceGrpc.newFutureStub(channel);

        System.out.println(runBenchmark(binConfigsPerRequest, maxConcurrentRequests, benchmarkDurationSeconds));
        executorService.shutdownNow();
    }

    // 200 671120.0
    // 400 1381120.0
    // 800 2172800.0


}
