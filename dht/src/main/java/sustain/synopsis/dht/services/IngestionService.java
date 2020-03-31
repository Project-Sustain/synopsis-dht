package sustain.synopsis.dht.services;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.IngestionRequestDispatcher;
import sustain.synopsis.dht.store.services.*;

public class IngestionService extends IngestionServiceGrpc.IngestionServiceImplBase {
    private final IngestionRequestDispatcher dispatcher;

    public IngestionService(IngestionRequestDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void ingest(IngestionRequest request, StreamObserver<IngestionResponse> responseObserver) {
        dispatcher.dispatch(request).thenAccept(resp -> {
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void terminateSession(TerminateSessionRequest request,
                                 StreamObserver<TerminateSessionResponse> responseObserver) {
        dispatcher.terminateSession(request).thenAccept(status -> {
            responseObserver.onNext(TerminateSessionResponse.newBuilder().setStatus(status).build());
            responseObserver.onCompleted();
        });
    }
}
