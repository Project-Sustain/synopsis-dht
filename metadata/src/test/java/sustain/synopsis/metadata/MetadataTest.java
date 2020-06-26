package sustain.synopsis.metadata;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MetadataTest {

    public static void main(String[] args) {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String datasetId = args[2];


        DatasetSessions.Builder sessionsBuilder = DatasetSessions.newBuilder()
                .setDatasetId(datasetId);
        for (int i = 3; i < args.length; i++) {
            sessionsBuilder.addSessionId(Long.parseLong(args[i]));
        }

        Channel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        MetadataServiceGrpc.MetadataServiceBlockingStub stub = MetadataServiceGrpc.newBlockingStub(channel);

        GetMetadataResponse resp = stub.getMetadata(GetMetadataRequest.newBuilder()
                .addDatasetSessions(sessionsBuilder.build())
                .build());

        for (ProtoBuffSerializedDatasetMetadata datasetMetadata : resp.getDatasetMetadataList()) {
            for (ProtoBuffSerializedSessionMetadata sessionMetadata : datasetMetadata.getSessionMetadataList()) {
                System.out.println(MetadataServiceRequestProcessor.getStringForEntry(datasetMetadata.getDatasetId(), sessionMetadata));
            }
        }

    }
}
