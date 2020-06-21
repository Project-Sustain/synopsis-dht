package sustain.synopsis.metadata;

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;

public class MetadataService extends MetadataServiceGrpc.MetadataServiceImplBase {

    private final Logger logger = Logger.getLogger(MetadataService.class);
    private final MetadataRequestProcessor dispatcher;

    public MetadataService(MetadataRequestProcessor dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override
    public void publishBinConfiguration(PublishBinConfigurationRequest request, StreamObserver<PublishBinConfigurationResponse> responseObserver) {
        dispatcher.process(request).thenAccept(resp -> {
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }).exceptionally(err -> {
            logger.error(err);
            responseObserver.onError(err);
            return null;
        });
    }

    @Override
    public void getBinConfigurationRequest(BinConfigurationRequest request, StreamObserver<BinConfigurationResponse> responseObserver) {
        dispatcher.process(request).thenAccept(resp -> {
            responseObserver.onNext(resp);
            responseObserver.onCompleted();
        }).exceptionally(err -> {
            logger.error(err);
            responseObserver.onError(err);
            return null;
        });
    }
}
