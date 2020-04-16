package sustain.synopsis.proxy.ingestion;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.Ring;
import sustain.synopsis.dht.services.ingestion.IngestionRequestProcessor;
import sustain.synopsis.dht.store.services.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class ProxyIngestionRequestProcessor implements IngestionRequestProcessor {

    static class IngestionResponseMergeHelper implements ResponseMergeHelper<IngestionResponse> {
        @Override
        public IngestionResponse getEmptyMessage() {
            return IngestionResponse.newBuilder().setStatus(true).build();
        }

        @Override
        public IngestionResponse merge(IngestionResponse base, IngestionResponse newResponse) {
            boolean newStatus = base.getStatus() && newResponse.getStatus();
            return base.toBuilder().mergeFrom(newResponse).setStatus(newStatus).build();
        }
    }

    static class TerminateSessionResponseMergeHelper implements ResponseMergeHelper<TerminateSessionResponse> {

        @Override
        public TerminateSessionResponse getEmptyMessage() {
            return TerminateSessionResponse.newBuilder().setStatus(true).build();
        }

        @Override
        public TerminateSessionResponse merge(TerminateSessionResponse base, TerminateSessionResponse newResponse) {
            boolean newStatus = base.getStatus() && newResponse.getStatus();
            return base.toBuilder().mergeFrom(newResponse).setStatus(newStatus).build();
        }
    }

    private static final int DEFAULT_MERGE_PARALLELISM = Runtime.getRuntime().availableProcessors() * 2;
    private final Map<String, IngestionServiceGrpc.IngestionServiceFutureStub> stubs = new ConcurrentHashMap<>();
    private final ExecutorService responseHandlers = Executors.newFixedThreadPool(DEFAULT_MERGE_PARALLELISM);
    private final IngestionResponseMergeHelper ingestionResponseHelper = new IngestionResponseMergeHelper();
    private final TerminateSessionResponseMergeHelper terminateSessionResponseHelper =
            new TerminateSessionResponseMergeHelper();
    private final Ring ring;

    public ProxyIngestionRequestProcessor() {
        ring = Context.getInstance().getRing();
    }

    // used for unit testing by injecting a mock ring
    ProxyIngestionRequestProcessor(Ring ring) {
        this.ring = ring;
    }

    @Override
    public CompletableFuture<IngestionResponse> process(IngestionRequest request) {
        CompletableFuture<IngestionResponse> future = new CompletableFuture<>();
        if (request.getStrandCount() == 0) { // handling empty ingestion requests
            future.complete(IngestionResponse.newBuilder().setDatasetId(request.getDatasetId())
                                             .setSessionId(request.getSessionId()).setMessageId(request.getMessageId())
                                             .setStatus(true).build());
            return future;
        }
        Map<String, IngestionRequest> splits = split(request);
        ResponseContainer<IngestionResponse> responseContainer =
                new ResponseContainer<>(splits.size(), ingestionResponseHelper,
                                        Math.min(DEFAULT_MERGE_PARALLELISM, splits.size()));
        splits.forEach((endpoint, split) -> dispatchSplit(getStub(endpoint), split, responseContainer, future));
        return future;
    }

    void dispatchSplit(IngestionServiceGrpc.IngestionServiceFutureStub stub, IngestionRequest splitRequest,
                       ResponseContainer<IngestionResponse> responseContainer,
                       CompletableFuture<IngestionResponse> future) {
        Futures.addCallback(stub.ingest(splitRequest), new FutureCallback<IngestionResponse>() {
            @Override
            public void onSuccess(@NullableDecl IngestionResponse result) {
                boolean complete = responseContainer.handleResponse(result);
                if (complete) {
                    future.complete(responseContainer.getMergedResponse());
                }
            }

            @Override
            public void onFailure(Throwable t) {
                future.completeExceptionally(t);
            }
        }, responseHandlers);
    }

    @Override
    public CompletableFuture<TerminateSessionResponse> process(TerminateSessionRequest request) {
        // send to all dht nodes
        ResponseContainer<TerminateSessionResponse> container =
                new ResponseContainer<>(stubs.size(), terminateSessionResponseHelper,
                                        Math.min(DEFAULT_MERGE_PARALLELISM, stubs.size()));
        CompletableFuture<TerminateSessionResponse> future = new CompletableFuture<>();
        stubs.values().forEach(stub -> {
            ListenableFuture<TerminateSessionResponse> listenableFuture = stub.terminateSession(request);
            Futures.addCallback(listenableFuture, new FutureCallback<TerminateSessionResponse>() {
                @Override
                public void onSuccess(@NullableDecl TerminateSessionResponse result) {
                    boolean complete = container.handleResponse(result);
                    if (complete) {
                        future.complete(container.getMergedResponse());
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    future.completeExceptionally(t);
                }
            }, responseHandlers);
        });
        return future;
    }

    Map<String, IngestionRequest> split(IngestionRequest request) {
        Map<String, IngestionRequest.Builder> splitBuilders = new HashMap<>();
        request.getStrandList().forEach(s -> {
            // look up the primary data holder
            String targetNode = ring.lookup(getKey(s));
            IngestionRequest.Builder builder;
            if (!splitBuilders.containsKey(targetNode)) {
                builder = IngestionRequest.newBuilder();
                builder.setDatasetId(request.getDatasetId()).setSessionId(request.getSessionId())
                       .setMessageId(request.getMessageId());
                splitBuilders.put(targetNode, builder);
            } else {
                builder = splitBuilders.get(targetNode);
            }
            builder.addStrand(s);
        });
        return splitBuilders.entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().build()));
    }

    String getKey(Strand strand) {
        return strand.getEntityId() + ":" + CommonUtil.epochToLocalDateTime(strand.getFromTs()).getMonthValue();
    }

    private IngestionServiceGrpc.IngestionServiceFutureStub getStub(String endpoint) {
        if (stubs.containsKey(endpoint)) {
            return stubs.get(endpoint);
        }
        synchronized (this) {
            if (stubs.containsKey(endpoint)) {
                return stubs.get(endpoint);
            }
            String[] splits = endpoint.split(":");
            Channel channel =
                    ManagedChannelBuilder.forAddress(splits[0], Integer.parseInt(splits[1])).usePlaintext().build();
            IngestionServiceGrpc.IngestionServiceFutureStub stub = IngestionServiceGrpc.newFutureStub(channel);
            stubs.put(endpoint, stub);
            return stub;
        }
    }
}
