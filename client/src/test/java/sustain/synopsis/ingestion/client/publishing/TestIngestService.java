package sustain.synopsis.ingestion.client.publishing;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.store.services.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class TestIngestService implements Runnable {

    final int port;
    final Map<String,String> hostMap;
    int totalStrandsReceived = 0;

    CountDownLatch latch = new CountDownLatch(1);

    public TestIngestService(int port) {
        this.port = port;
        this.hostMap = new HashMap<>();
    }

    public TestIngestService(int port, Map<String, String> hostMap) {
        this.port = port;
        this.hostMap = hostMap;
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(args[0]);
        Map<String, String> hostMapping = new HashMap<>();
        if (args.length > 1) {
            hostMapping.put("b", "localhost:44002");
        }

        TestIngestService testIngestService = new TestIngestService(port, hostMapping);
        testIngestService.run();
    }

    @Override
    public void run() {
        try {
            Server server = ServerBuilder
                    .forPort(port)
                    .addService(new MyIngestService())
                    .build()
                    .start();
            latch.countDown();

            server.awaitTermination();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private class MyIngestService extends IngestionServiceGrpc.IngestionServiceImplBase {

        private Set<String> sentMappings = new HashSet<>(hostMap.size());

        private List<NodeMapping> processStrands(List<Strand> strands) {
            totalStrandsReceived += strands.size();
            List<NodeMapping> mappings = new ArrayList<>();
            for (Strand s : strands) {
                String key = s.getEntityId();
                String nodeMapping = hostMap.get(key);
                if (nodeMapping != null && !sentMappings.contains(key)) {
                    mappings.add(NodeMapping.newBuilder()
                            .setEntityId(key)
                            .setDhtNodeAddress(nodeMapping)
                            .build());
                    sentMappings.add(key);
                }
            }
            return mappings;
        }

        @Override
        public void ingest(IngestionRequest request, StreamObserver<IngestionResponse> responseObserver) {
            IngestionResponse response = IngestionResponse.newBuilder()
                        .setDatasetId(request.getDatasetId())
                        .setSessionId(request.getSessionId())
                        .setStatus(true)
                        .addAllMapping(processStrands(request.getStrandList()))
                        .build();

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void terminateSession(TerminateSessionRequest request, StreamObserver<TerminateSessionResponse> responseObserver) {
            TerminateSessionResponse response = TerminateSessionResponse.newBuilder()
                    .setStatus(true)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

    }

}
