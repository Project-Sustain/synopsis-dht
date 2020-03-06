package sustain.synopsis.ingestion.client.core;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.ingestion.client.connectors.DataConnector;
import sustain.synopsis.ingestion.client.connectors.DataParser;
import sustain.synopsis.ingestion.client.connectors.file.FileDataConnector;
import sustain.synopsis.metadata.DatasetServiceGrpc;
import sustain.synopsis.metadata.DatasetServiceGrpc.DatasetServiceBlockingStub;
import sustain.synopsis.metadata.DatasetServiceOuterClass.GetDatasetSessionRequest;
import sustain.synopsis.metadata.DatasetServiceOuterClass.GetDatasetSessionResponse;
import sustain.synopsis.metadata.DatasetServiceOuterClass.Session;
import sustain.synopsis.sketch.dataset.Quantizer;
import synopsis2.client.IngestionConfig;
import synopsis2.client.Util;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class Client {

    public static String metadataServiceHost = "localhost";
    public static int metadataServicePort = 44097;
    public static void ingest(DataConnector dc, IngestionTaskManager tm) {
        tm.start();
        dc.init();
        dc.start();
        tm.awaitCompletion();
        dc.terminate();
    }

    public static IngestionConfig fetchIngestionConfig(String datasetId, long sessionId) throws IOException {
        Channel channel = ManagedChannelBuilder.forAddress(metadataServiceHost,metadataServicePort)
                .usePlaintext()
                .build();
        DatasetServiceBlockingStub stub = DatasetServiceGrpc.newBlockingStub(channel);

        GetDatasetSessionRequest datasetSessionRequest = GetDatasetSessionRequest.newBuilder()
                .setDatasetId(datasetId)
                .setSessionId(sessionId)
                .build();
        GetDatasetSessionResponse datasetSession = stub.getDatasetSession(datasetSessionRequest);

        Session session = datasetSession.getSession();

        Map<String, Quantizer> stringQuantizerMap = Util.quantizerMapFromString(session.getBinConfig());
        int geohashLength = session.getGeohashLength();
        Duration temporalBracketLength = Duration.ofMillis(session.getTemporalBracketLength());

        return new IngestionConfig(stringQuantizerMap, geohashLength, temporalBracketLength);
    }

    public static File[] getFileListFromArgs(String[] args) {
        File[] files = new File[args.length-3];
        for (int i = 3; i < args.length; i++) {
            files[i-3] = (new File(args[i]));
        }
        return files;
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException, IllegalAccessException, InstantiationException {
        if (args.length < 4) {
            System.out.println("usage: datasetId sessionId FileParserClassName files...");
            return;
        }

        String datasetId = args[0];
        long sessionId = Long.parseLong(args[1]);
        IngestionConfig config = fetchIngestionConfig(datasetId, sessionId);

        String fileParserClassName = args[2];
        DataParser dataParser = (DataParser) Class.forName(fileParserClassName).newInstance();
        File[] files = getFileListFromArgs(args);

        StrandPublisher strandPublisher = new StrandPublisherImpl();

        IngestionTaskManager ingestionTaskManager = new IngestionTaskManager(
                4,
                strandPublisher,
                config.getQuantizers(),
                config.getTemporalGranularity());

        DataConnector dataConnector = new FileDataConnector(dataParser, ingestionTaskManager, files);

        ingest(dataConnector, ingestionTaskManager);
    }

}
