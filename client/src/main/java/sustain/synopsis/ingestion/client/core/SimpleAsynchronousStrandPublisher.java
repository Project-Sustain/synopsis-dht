package sustain.synopsis.ingestion.client.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
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

public class SimpleAsynchronousStrandPublisher implements StrandPublisher {

    private final String datasetId;
    private final long sessionId;

    IngestionServiceFutureStub stub;
    ExecutorService executorService = Executors.newSingleThreadExecutor();

    long sent = 0;
    long received = 0;

    private FutureCallback<IngestionResponse> ingestionFutureCallback = new FutureCallback<IngestionResponse>() {
        @Override
        public void onSuccess(@NullableDecl IngestionResponse result) {
            received++;
        }

        @Override
        public void onFailure(Throwable t) {

        }
    };

    public SimpleAsynchronousStrandPublisher(String address, String datasetId, long sessionId) {
        stub = getStubForAddress(address);
        this.datasetId = datasetId;
        this.sessionId = sessionId;
    }

    IngestionServiceFutureStub getStubForAddress(String address) {
        String host = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);

        Channel channel = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext()
                .build();

        return IngestionServiceGrpc.newFutureStub(channel);
    }

    List<sustain.synopsis.dht.store.services.Strand> getConvertedStrandList(Collection<Strand> strands) {
        List<sustain.synopsis.dht.store.services.Strand> convertedStrands = new ArrayList<>(strands.size());
        for (Strand s : strands) {
            convertedStrands.add(DHTStrandPublisher.convertStrand(s));
        }
        return convertedStrands;
    }

    @Override
    public void publish(long messageId, Collection<Strand> strands) {
        IngestionRequest request = IngestionRequest.newBuilder()
                .setMessageId(messageId)
                .setDatasetId(datasetId)
                .setSessionId(sessionId)
                .addAllStrand(getConvertedStrandList(strands))
                .build();

        ListenableFuture<IngestionResponse> ingestResponseFuture = stub.ingest(request);
        sent++;
        Futures.addCallback(ingestResponseFuture, ingestionFutureCallback, executorService);
    }

}
