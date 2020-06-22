package sustain.synopsis.benchmarks.metadata;

import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.metadata.*;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class MetadataServicePublishBenchmark {


    static MetadataServiceGrpc.MetadataServiceFutureStub stub;
    static ExecutorService executorService = Executors.newSingleThreadExecutor();
    static long requestsFinished;

    static ProtoBuffSerializedBinConfiguration[] binConfigurations = new ProtoBuffSerializedBinConfiguration[]{
        ProtoBuffSerializedBinConfiguration.newBuilder()
                .setFeatureName("feature1")
                .addValues(1.0f).addValues(2.0f).addValues(3.0f).addValues(4.0f).addValues(5.0f)
                .build(),
        ProtoBuffSerializedBinConfiguration.newBuilder()
                .setFeatureName("feature2")
                .addValues(1.0f).addValues(2.0f).addValues(3.0f).addValues(4.0f).addValues(5.0f)
                .build(),
        ProtoBuffSerializedBinConfiguration.newBuilder()
                .setFeatureName("feature3")
                .addValues(1.0f).addValues(2.0f).addValues(3.0f).addValues(4.0f).addValues(5.0f)
                .build()
    };

    static double runBenchmark(int maxConcurrentRequests, int benchmarkDurationSeconds, String datasetId) throws InterruptedException {
        requestsFinished = 0;

        AtomicInteger sessionId = new AtomicInteger(0);
        Semaphore semaphore = new Semaphore(maxConcurrentRequests);
        long endTime = System.currentTimeMillis() + benchmarkDurationSeconds * 1000;
        while (System.currentTimeMillis() < endTime) {
            semaphore.acquire();
            if (System.currentTimeMillis() >= endTime) {
                break;
            }

            ListenableFuture<PublishMetadataResponse> responseListenableFuture = stub.publishMetadata(
                    PublishMetadataRequest.newBuilder()
                            .setDatasetId(datasetId)
                            .setSessionMetadata(ProtoBuffSerializedSessionMetadata.newBuilder()
                                    .setSessionId(sessionId.getAndIncrement())
                                    .addAllBinConfiguration(Arrays.stream(binConfigurations).collect(Collectors.toList()))
                                    .build())
                            .build()
            );

            responseListenableFuture.addListener(() -> {
                try {
                    PublishMetadataResponse resp = responseListenableFuture.get();
                    resp.getStatus();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
                requestsFinished++;
                semaphore.release();

            }, executorService);
        }

        double msgPerSec = (double)requestsFinished/ benchmarkDurationSeconds;
        return msgPerSec;
    }

    public static void main(String[] args) throws InterruptedException {
        String metadataHost = args[0];
        int metadataPort = Integer.parseInt(args[1]);
        int maxConcurrentRequests = Integer.parseInt(args[2]);
        int benchmarkDurationSeconds = Integer.parseInt(args[3]);

        Channel channel = ManagedChannelBuilder.forAddress(metadataHost, metadataPort)
                .usePlaintext()
                .build();
        stub = MetadataServiceGrpc.newFutureStub(channel);

        System.out.println(runBenchmark(maxConcurrentRequests, benchmarkDurationSeconds, "myDatasetId"));
        executorService.shutdownNow();
    }
}
