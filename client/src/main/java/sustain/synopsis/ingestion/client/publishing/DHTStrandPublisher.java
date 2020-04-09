package sustain.synopsis.ingestion.client.publishing;

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
import sustain.synopsis.dht.store.services.NodeMapping;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

public class DHTStrandPublisher implements StrandPublisher {

    public static final int channelSendLimit = 1;

    private final ExecutorService responseExecutorService = Executors.newSingleThreadExecutor();

    final String datasetId;
    final long sessionId;

    final Map<String, MyChannel> myChannelMap = new HashMap<>();
    final MyChannel defaultMyChannel;

    public DHTStrandPublisher(String initialAddress, String datasetId, long sessionId) {
        this.datasetId = datasetId;
        this.sessionId = sessionId;
        this.defaultMyChannel = new MyChannel(initialAddress);
    }

    synchronized Map<MyChannel, List<sustain.synopsis.dht.store.services.Strand>> mapStrandsToChannels(Iterable<Strand> strands) {
        Map<MyChannel, List<sustain.synopsis.dht.store.services.Strand>> strandsForStub = new HashMap<>();
        for (Strand s : strands) {
            MyChannel myChannel = myChannelMap.getOrDefault(s.getGeohash(), defaultMyChannel);
            List<sustain.synopsis.dht.store.services.Strand> strandList = strandsForStub.computeIfAbsent(myChannel, k -> new ArrayList<>());
            strandList.add(convertStrand(s));
        }
        return strandsForStub;
    }

    synchronized void processNodeMappings(List<NodeMapping> mappings) {
        for (NodeMapping mapping : mappings) {
            String nodeAddress = mapping.getDhtNodeAddress();
            if (!myChannelMap.containsKey(nodeAddress)) {
                myChannelMap.put(mapping.getEntityId(), new MyChannel(nodeAddress));
            }
        }
    }

    @Override
    public void publish(Iterable<Strand> strands) {
        Map<MyChannel, List<sustain.synopsis.dht.store.services.Strand>> strandsForStubMap = mapStrandsToChannels(strands);
        for (MyChannel myChannel : strandsForStubMap.keySet()) {
            myChannel.publish(strandsForStubMap.get(myChannel));
        }
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

        void publish(List<sustain.synopsis.dht.store.services.Strand> strands) {
            try {
                limiter.acquire();
                IngestionRequest request = IngestionRequest.newBuilder()
                        .setDatasetId(datasetId)
                        .setSessionId(sessionId)
                        .addAllStrand(strands)
                        .build();

                ListenableFuture<IngestionResponse> responseFuture = futureStub.ingest(request);
                Futures.addCallback(responseFuture, new FutureCallback<IngestionResponse>() {
                    @Override
                    public void onSuccess(@NullableDecl IngestionResponse result) {
                        processNodeMappings(result.getMappingList());
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

    }


    static sustain.synopsis.dht.store.services.Strand convertStrand(Strand strand) {
        return sustain.synopsis.dht.store.services.Strand.newBuilder().setEntityId(strand.getGeohash())
                .setToTs(strand.getToTimestamp())
                .setFromTs(strand.getFromTimeStamp())
                .setBytes(strand.serializeAsProtoBuff()).build();
    }

}
