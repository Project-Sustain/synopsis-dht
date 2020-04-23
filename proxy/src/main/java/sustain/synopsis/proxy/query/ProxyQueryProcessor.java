package sustain.synopsis.proxy.query;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.Ring;
import sustain.synopsis.dht.services.query.QueryContainer;
import sustain.synopsis.dht.services.query.QueryProcessor;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;
import sustain.synopsis.dht.store.services.TargetedQueryServiceGrpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

public class ProxyQueryProcessor implements QueryProcessor {
    private Map<String, TargetedQueryServiceGrpc.TargetedQueryServiceStub> stubs = new ConcurrentHashMap<>();
    private final Ring ring;

    public ProxyQueryProcessor() {
        this.ring = Context.getInstance().getRing();
    }

    @Override
    public CompletableFuture<Boolean> process(TargetQueryRequest queryRequest,
                                              StreamObserver<TargetQueryResponse> responseObserver) {
        Set<String> endpoints = ring.getUniqueEndpoints();
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        QueryContainer container =
                new QueryContainer(new CountDownLatch(endpoints.size()), future, responseObserver, new HashSet<>(),
                                   1024);
        container.startStreamPublisher();
        endpoints.forEach(endpoint -> {
            getStub(endpoint).query(queryRequest, new StreamObserver<TargetQueryResponse>() {
                @Override
                public void onNext(TargetQueryResponse value) {
                    container.write(value);
                }

                @Override
                public void onError(Throwable t) {
                    responseObserver.onError(t);
                    container.reportReaderTaskComplete(false);
                }

                @Override
                public void onCompleted() {
                    container.reportReaderTaskComplete(true);
                }
            });
        });
        return future;
    }

    private TargetedQueryServiceGrpc.TargetedQueryServiceStub getStub(String endpoint) {
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
            TargetedQueryServiceGrpc.TargetedQueryServiceStub stub = TargetedQueryServiceGrpc.newStub(channel);
            stubs.put(endpoint, stub);
            return stub;
        }
    }
}
