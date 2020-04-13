package sustain.synopsis.ingestion.client.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceFutureStub;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DHTStrandPublisher implements StrandPublisher {

    private final ExecutorService senderExecutorService = Executors.newFixedThreadPool(4);
    private final ExecutorService responseExecutorService = Executors.newSingleThreadExecutor();

    private final String datasetId;
    private final long sessionId;
    private final IngestionServiceFutureStub proxyStub;

    public DHTStrandPublisher(String proxyAddress, String datasetId, long sessionId) {
        proxyStub = getStubForAddress(proxyAddress);
        this.datasetId = datasetId;
        this.sessionId = sessionId;
    }

    IngestionServiceFutureStub getStubForAddress(String address) {
        String host = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);

        Channel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();

        return IngestionServiceGrpc.newFutureStub(channel);
    }

    static sustain.synopsis.dht.store.services.Strand convertStrand(Strand strand) {
        return sustain.synopsis.dht.store.services.Strand.newBuilder().setEntityId(strand.getGeohash())
                                                         .setToTs(strand.getToTimestamp())
                                                         .setFromTs(strand.getFromTimeStamp())
                                                         .setBytes(strand.serializeAsProtoBuff()).build();
    }

    @Override
    public void publish(long messageId, Collection<Strand> strands) {
        List<sustain.synopsis.dht.store.services.Strand> convertedStrandList = new ArrayList<>(strands.size());
        strands.forEach(s -> convertedStrandList.add(convertStrand(s)));

        IngestionRequest req = IngestionRequest.newBuilder().setDatasetId(datasetId).setSessionId(sessionId)
                                               .addAllStrand(convertedStrandList).build();
        ListenableFuture<IngestionResponse> ingestFuture = proxyStub.ingest(req);

        Futures.addCallback(ingestFuture, new FutureCallback<IngestionResponse>() {
            @Override
            public void onSuccess(IngestionResponse result) {

            }

            @Override
            public void onFailure(Throwable t) {

            }
        }, responseExecutorService);
    }
}
