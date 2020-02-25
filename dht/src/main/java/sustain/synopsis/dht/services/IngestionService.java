package sustain.synopsis.dht.services;

import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.IngestionRequestDispatcher;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class IngestionService extends IngestionServiceGrpc.IngestionServiceImplBase {
    private final IngestionRequestDispatcher dispatcher;


    public IngestionService() throws StorageException {
        this.dispatcher = new IngestionRequestDispatcher();
    }

    @Override
    public void ingest(IngestionRequest request, StreamObserver<IngestionResponse> responseObserver) {
        CompletableFuture<IngestionResponse> future = dispatcher.dispatch(request);
        try {
            IngestionResponse response = future.get();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}
