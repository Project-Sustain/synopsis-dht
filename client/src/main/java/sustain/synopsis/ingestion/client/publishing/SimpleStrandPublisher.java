package sustain.synopsis.ingestion.client.publishing;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceBlockingStub;

import java.util.List;
import org.apache.log4j.Logger;

public class SimpleStrandPublisher implements StrandPublisher {

    Logger logger = Logger.getLogger(SimpleStrandPublisher.class);
    private final String datasetId;
    private final long sessionId;

    IngestionServiceBlockingStub stub;
    long totalStrandsPublished = 0;

    public SimpleStrandPublisher(String address, String datasetId, long sessionId) {
        stub = getStubForAddress(address);
        this.datasetId = datasetId;
        this.sessionId = sessionId;
    }

    IngestionServiceBlockingStub getStubForAddress(String address) {
        String host = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);

        Channel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        return IngestionServiceGrpc.newBlockingStub(channel);
    }

    public long getStrandsPublishedCount() {
        return totalStrandsPublished;
    }

    @Override
    public void publish(long messageId, Iterable<Strand> strands) {
        List<sustain.synopsis.dht.store.services.Strand> convertedStrandList = SimpleAsynchronousStrandPublisher.getConvertedStrandList(strands);
        totalStrandsPublished += convertedStrandList.size();

        IngestionRequest request = IngestionRequest.newBuilder()
                .setMessageId(messageId)
                .setDatasetId(datasetId)
                .setSessionId(sessionId)
                .addAllStrand(convertedStrandList)
                .build();

        IngestionResponse response = stub.ingest(request);
        logger.info("published "+convertedStrandList.size()+" strands with status "+response.getStatus());
    }

    @Override
    public void terminateSession() {
        TerminateSessionResponse resp =
                stub.terminateSession(TerminateSessionRequest.newBuilder().
                        setDatasetId(datasetId).
                        setSessionId(sessionId).
                        build());
    }

}
