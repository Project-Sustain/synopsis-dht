package sustain.synopsis.ingestion.client.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.checkerframework.checker.nullness.compatqual.NullableDecl;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceFutureStub;
import sustain.synopsis.dht.store.services.NodeMapping;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.*;

public class DHTStrandPublisher implements StrandPublisher {

    public final int channelSendLimit = 3;
    //TODO these maps will need some locking
    private final Map<String, IngestionServiceFutureStub> geohashStubMap = new HashMap<>();
    private final Map<IngestionServiceFutureStub, Semaphore> sendLimitMap = new HashMap<>();

    private final ExecutorService senderExecutorService = Executors.newFixedThreadPool(4);
    private final ExecutorService responseExecutorService = Executors.newSingleThreadExecutor();

    private final String datasetId;
    private final long sessionId;
    private final IngestionServiceFutureStub defaultStub;

    public DHTStrandPublisher(String initialAddress, String datasetId, long sessionId) {
        defaultStub = getStubForAddress(initialAddress);
        sendLimitMap.put(defaultStub, new Semaphore(channelSendLimit));
        this.datasetId = datasetId;
        this.sessionId = sessionId;
    }

    // https://stackoverflow.com/a/30968827
    static byte[] serializeToBytes(Strand s) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); SerializationOutputStream sos = new SerializationOutputStream(new ObjectOutputStream(bos))) {
            s.serialize(sos);
            return bos.toByteArray();
        }
    }

    IngestionServiceFutureStub getStubForAddress(String address) {
        String host = address.split(":")[0];
        int port = Integer.parseInt(address.split(":")[1]);

        Channel channel = ManagedChannelBuilder.forAddress(host,port)
                .usePlaintext()
                .build();

        return IngestionServiceGrpc.newFutureStub(channel);
    }

    Map<IngestionServiceFutureStub, List<Strand>> getStrandsForStubMap(Collection<Strand> strands) {
        Map<IngestionServiceFutureStub, List<Strand>> strandsForStub = new HashMap<>();
        for (Strand s : strands) {
            IngestionServiceFutureStub stub = geohashStubMap.getOrDefault(s.getGeohash(), defaultStub);

            List<Strand> strandList = strandsForStub.computeIfAbsent(stub, k -> new ArrayList<>());
            strandList.add(s);
        }
        return strandsForStub;
    }

    static sustain.synopsis.dht.store.services.Strand convertStrand(Strand strand) {
        try {
            return sustain.synopsis.dht.store.services.Strand.newBuilder()
                    .setEntityId(strand.getGeohash())
                    .setToTs(strand.getToTimestamp())
                    .setFromTs(strand.getFromTimeStamp())
                    .setBytes(ByteString.copyFrom(serializeToBytes(strand)))
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    void processNodeMapping(NodeMapping mapping) {
        String nodeAddress = mapping.getDhtNodeAddress();
        if (!geohashStubMap.containsKey(nodeAddress)) {
            IngestionServiceFutureStub stubForAddress = getStubForAddress(nodeAddress);
            geohashStubMap.put(mapping.getEntityId(), stubForAddress);
            sendLimitMap.put(stubForAddress, new Semaphore(channelSendLimit));
        }
    }


    void publishStrandsForStub(IngestionServiceFutureStub stub, List<Strand> strands) {
        try {
            Semaphore limiter = sendLimitMap.get(stub);
            limiter.acquire();

            List<sustain.synopsis.dht.store.services.Strand> convertedStrandList = new ArrayList<>(strands.size());
            strands.stream().forEach(s -> convertedStrandList.add(convertStrand(s)));

            IngestionRequest req = IngestionRequest.newBuilder()
                    .setDatasetId(datasetId)
                    .setSessionId(sessionId)
                    .addAllStrand(convertedStrandList)
                    .build();
            ListenableFuture<IngestionResponse> ingestFuture = stub.ingest(req);

            Futures.addCallback(ingestFuture, new FutureCallback<IngestionResponse>() {
                @Override
                public void onSuccess(IngestionResponse result) {
                   for (int i = 0; i < result.getMappingCount(); i++) {
                       processNodeMapping(result.getMapping(i));
                   }
                }

                @Override
                public void onFailure(Throwable t) {

                }
            }, responseExecutorService);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void publish(Collection<Strand> strands) {
        Map<IngestionServiceFutureStub, List<Strand>> strandsForStub = getStrandsForStubMap(strands);
        for (IngestionServiceFutureStub stub : strandsForStub.keySet()) {
            publishStrandsForStub(stub, strandsForStub.get(stub));
        }
    }

}
