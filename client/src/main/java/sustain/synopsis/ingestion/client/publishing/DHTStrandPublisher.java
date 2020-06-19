package sustain.synopsis.ingestion.client.publishing;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceFutureStub;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class DHTStrandPublisher implements StrandPublisher {

    public static final int channelSendLimit = 4;

    private final ExecutorService responseExecutorService = Executors.newSingleThreadExecutor();

    final String datasetId;
    final long sessionId;

    final MyChannel defaultMyChannel;

    public DHTStrandPublisher(String initialAddress, String datasetId, long sessionId) {
        this.datasetId = datasetId;
        this.sessionId = sessionId;
        this.defaultMyChannel = new MyChannel(initialAddress);
    }


    @Override
    public void publish(long messageId, Iterable<Strand> strands) {
        List<sustain.synopsis.dht.store.services.Strand> strandsList = new ArrayList<>();
        strands.forEach(s -> strandsList.add(convertStrand(s)));
        defaultMyChannel.publish(messageId, strandsList);
    }

    @Override
    public void terminateSession() {
        defaultMyChannel.terminateSession();
    }

    @Override
    public long getStrandsPublishedCount() {
        return 0;
    }

    class MyChannel {
        Semaphore limiter = new Semaphore(channelSendLimit);
        IngestionServiceFutureStub futureStub;

        public MyChannel(String address) {
            String host = address.split(":")[0];
            int port = Integer.parseInt(address.split(":")[1]);
            Channel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
            this.futureStub = IngestionServiceGrpc.newFutureStub(channel);
        }

        void publish(long messageId, List<sustain.synopsis.dht.store.services.Strand> strands) {
            try {
                limiter.acquire();
                IngestionRequest request = IngestionRequest.newBuilder()
                        .setDatasetId(datasetId)
                        .setSessionId(sessionId)
                        .setSessionId(messageId)
                        .addAllStrand(strands)
                        .build();

                ListenableFuture<IngestionResponse> responseFuture = futureStub.ingest(request);
                Futures.addCallback(responseFuture, new FutureCallback<IngestionResponse>() {
                    @Override
                    public void onSuccess(@NullableDecl IngestionResponse result) {
                        limiter.release();
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        limiter.release();
                    }
                }, responseExecutorService);


            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        void terminateSession() {
            TerminateSessionRequest request = TerminateSessionRequest.newBuilder()
                    .setDatasetId(datasetId)
                    .setSessionId(sessionId)
                    .build();

            futureStub.terminateSession(request);
        }

    }


    static sustain.synopsis.dht.store.services.Strand convertStrand(Strand strand) {
        return sustain.synopsis.dht.store.services.Strand.newBuilder().setEntityId(strand.getGeohash())
                .setToTs(strand.getToTimestamp())
                .setFromTs(strand.getFromTimeStamp())
                .setBytes(strand.serializeAsProtoBuff()).build();
    }

}
