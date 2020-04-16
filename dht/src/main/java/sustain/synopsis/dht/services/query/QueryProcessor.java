package sustain.synopsis.dht.services.query;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.concurrent.CompletableFuture;

public interface QueryProcessor {
    CompletableFuture<Boolean> process(TargetQueryRequest queryRequest,
                                        StreamObserver<TargetQueryResponse> responseObserver);
}
