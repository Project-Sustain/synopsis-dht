package sustain.synopsis.ingestion.client.core;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceBlockingStub;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SimpleStrandPublisher implements StrandPublisher {

    private final String datasetId;
    private final long sessionId;

    IngestionServiceBlockingStub stub;

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

    List<sustain.synopsis.dht.store.services.Strand> getConvertedStrandList(Collection<Strand> strands) {
        List<sustain.synopsis.dht.store.services.Strand> convertedStrands = new ArrayList<>(strands.size());
        for (Strand s : strands) {
            convertedStrands.add(DHTStrandPublisher.convertStrand(s));
        }
        return convertedStrands;
    }

    @Override
    public void publish(Collection<Strand> strands) {
        IngestionRequest request = IngestionRequest.newBuilder()
                .setDatasetId(datasetId)
                .setSessionId(sessionId)
                .addAllStrand(getConvertedStrandList(strands))
                .build();

        IngestionResponse response = stub.ingest(request);
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
