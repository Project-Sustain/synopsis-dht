package sustain.synopsis.dht.services;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.query.QueryCoordinator;
import sustain.synopsis.dht.store.query.QueryException;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;
import sustain.synopsis.dht.store.services.TargetedQueryServiceGrpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class TargetedQueryService extends TargetedQueryServiceGrpc.TargetedQueryServiceImplBase {
    private final QueryCoordinator coordinator;

    public TargetedQueryService(NodeStore nodeStore, ExecutorService readers) {
        this.coordinator = new QueryCoordinator(nodeStore, readers);
    }

    @Override
    public void query(TargetQueryRequest request, StreamObserver<TargetQueryResponse> responseObserver) {
        try {
            CompletableFuture<Boolean> future = coordinator.schedule(request, responseObserver);
            future.thenAccept(status -> {
                responseObserver.onCompleted();
            });
        } catch (QueryException e) { // todo: handle the error
            e.printStackTrace();
        }
    }
}
