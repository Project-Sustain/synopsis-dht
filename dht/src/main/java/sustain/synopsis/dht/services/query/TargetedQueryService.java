package sustain.synopsis.dht.services.query;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;
import sustain.synopsis.dht.store.services.TargetedQueryServiceGrpc;

import java.util.concurrent.CompletableFuture;

public class TargetedQueryService extends TargetedQueryServiceGrpc.TargetedQueryServiceImplBase {
    private final QueryProcessor processor;

    public TargetedQueryService(QueryProcessor processor) {
        this.processor = processor;
    }

    @Override
    public void query(TargetQueryRequest request, StreamObserver<TargetQueryResponse> responseObserver) {
        CompletableFuture<Boolean> future = processor.process(request, responseObserver);
        future.thenAccept(status -> {
            responseObserver.onCompleted();
        }).exceptionally(err -> {
            responseObserver.onError(err);
            responseObserver.onCompleted();
            return null;
        });
    }
}
