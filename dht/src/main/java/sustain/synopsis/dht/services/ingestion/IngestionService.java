package sustain.synopsis.dht.services.ingestion;

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.services.*;

public class IngestionService extends IngestionServiceGrpc.IngestionServiceImplBase {
    private final Logger logger = Logger.getLogger(IngestionService.class);
    private final IngestionRequestProcessor dispatcher;

    public IngestionService(IngestionRequestProcessor dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void ingest(IngestionRequest request, StreamObserver<IngestionResponse> responseObserver) {
        dispatcher.process(request).thenAccept(resp -> {
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }).exceptionally(err -> {
            logger.error(err);
            responseObserver.onError(err);
            responseObserver.onCompleted();
            return null;
        });
    }

    @Override
    public void terminateSession(TerminateSessionRequest request,
                                 StreamObserver<TerminateSessionResponse> responseObserver) {
        dispatcher.process(request).thenAccept(response -> {
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }).exceptionally(err -> {
            logger.error(err);
            responseObserver.onError(err);
            responseObserver.onCompleted();
            return null;
        });
    }
}
