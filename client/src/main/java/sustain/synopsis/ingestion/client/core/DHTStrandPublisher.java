package sustain.synopsis.ingestion.client.core;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceFutureStub;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.*;

public class DHTStrandPublisher implements StrandPublisher {

    public final int channelSendLimit = 50;
    private final HashMap<String, IngestionServiceFutureStub> geohashStubMap = new HashMap<>();
    private final HashMap<IngestionServiceFutureStub, Semaphore> sendLimitMap = new HashMap<>();
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final String datasetId;
    private final long sessionId;
    private final IngestionServiceFutureStub defaultStub;

    public DHTStrandPublisher(String initialAddress, String datasetId, long sessionId) {
        defaultStub = getStubForAddress(initialAddress);
        sendLimitMap.put(defaultStub, new Semaphore(channelSendLimit));
        this.datasetId = datasetId;
        this.sessionId = sessionId;
    }

    private IngestionServiceFutureStub getStubForAddress(String address) {
        String host = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);

        Channel channel = ManagedChannelBuilder.forAddress(host,port)
                .usePlaintext()
                .build();

        return IngestionServiceGrpc.newFutureStub(channel);
    }

    // https://stackoverflow.com/a/30968827
    public static byte[] serializeToBytes(Object o) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(o);
            return bos.toByteArray();
        }
    }

    private IngestionRequest getIngestionRequestForStrand(Strand strand) throws IOException {
        ByteString strandBytes = ByteString.copyFrom(serializeToBytes(strand));
        return IngestionRequest.newBuilder()
                .setDatasetId(datasetId)
                .setSessionId(sessionId)
                .setFromTS(strand.getFromTimeStamp())
                .setToTS(strand.getToTimestamp())
                .setEntityId(strand.getGeohash())
                .setStrand(strandBytes)
                .build();
    }

    @Override
    public void publish(Collection<Strand> strands) {
        for (Strand strand : strands) {
            try {
                IngestionServiceFutureStub stub = geohashStubMap.getOrDefault(strand.getGeohash(), defaultStub);
                Semaphore limiter = sendLimitMap.get(stub);
                limiter.acquire();

                IngestionRequest req = getIngestionRequestForStrand(strand);
                ListenableFuture<IngestionResponse> ingestFuture = stub.ingest(req);
                ingestFuture.addListener(() -> {
                    limiter.release();

                    // TODO FIX THIS
                    try {
                        IngestionResponse resp = ingestFuture.get();
                        String primary = resp.getPrimary();

                        if (!geohashStubMap.containsKey(primary)) {
                            IngestionServiceFutureStub stubForAddress = getStubForAddress(primary);
                            geohashStubMap.put(strand.getGeohash(), stubForAddress);
                            sendLimitMap.put(stubForAddress, new Semaphore(channelSendLimit));
                        }

                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }, executorService);

            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }

}
