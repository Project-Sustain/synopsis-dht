package sustain.synopsis.dht.services;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.IngestionRequestDispatcher;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc;

public class IngestionService extends IngestionServiceGrpc.IngestionServiceImplBase {
    private final IngestionRequestDispatcher dispatcher;


    public IngestionService() throws StorageException {
        this.dispatcher = new IngestionRequestDispatcher();
    }

    // used for unit testing and benchmarking
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
}
