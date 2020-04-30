package sustain.synopsis.dht.services.query;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

/**
 * Sends the responses back to the client via Grpc.
 */
public class GrpcQueryResponseHandler implements QueryResponseHandler {

    private final StreamObserver<TargetQueryResponse> responseObserver;

    public GrpcQueryResponseHandler(StreamObserver<TargetQueryResponse> responseObserver) {
        this.responseObserver = responseObserver;
    }

    @Override
    public void handleResponse(TargetQueryResponse targetQueryResponse) {
        this.responseObserver.onNext(targetQueryResponse);
    }

    @Override
    public void handleError(Throwable t) {
        this.responseObserver.onError(t);
    }
}
