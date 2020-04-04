package sustain.synopsis.samples.client.noaa;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.dht.store.services.IngestionServiceGrpc.IngestionServiceFutureStub;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NoaaReceiverTestClient {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(args[0]);

        Server server = ServerBuilder
                .forPort(port)
                .addService(new MyIngestService())
                .build()
                .start();

        server.awaitTermination();
    }

    private static class MyIngestService extends IngestionServiceGrpc.IngestionServiceImplBase {

        long totalReceived = 0;

        @Override
        public void ingest(IngestionRequest request, StreamObserver<IngestionResponse> responseObserver) {
            totalReceived += request.getStrandCount();
            if (totalReceived % 10000 == 0) {
                System.out.println("Total Received: "+totalReceived);
            }

            IngestionResponse response = IngestionResponse.newBuilder()
                    .setDatasetId(request.getDatasetId())
                    .setSessionId(request.getSessionId())
                    .setStatus(true)
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
