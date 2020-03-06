package sustain.synopsis.ingestion.client.core;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.synopsis.ingestion.client.connectors.DataConnector;
import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.connectors.file.FileDataConnector;
import sustain.synopsis.metadata.DatasetServiceGrpc;
import sustain.synopsis.metadata.DatasetServiceGrpc.DatasetServiceBlockingStub;
import sustain.synopsis.metadata.DatasetServiceOuterClass.GetDatasetSessionRequest;
import sustain.synopsis.metadata.DatasetServiceOuterClass.GetDatasetSessionResponse;
import sustain.synopsis.metadata.DatasetServiceOuterClass.Session;
import sustain.synopsis.sketch.dataset.Quantizer;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

public class Client {

    public static String metadataServiceHost = "localhost";
    public static int metadataServicePort = 44097;

    public static SessionSchema fetchSessionSchema(String datasetId, long sessionId) throws IOException {
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

        return new SessionSchema(stringQuantizerMap, geohashLength, temporalBracketLength);
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
        SessionSchema config = fetchSessionSchema(datasetId, sessionId);

        String fileParserClassName = args[2];
        FileParser fileParser = (FileParser) Class.forName(fileParserClassName).newInstance();
        File[] files = getFileListFromArgs(args);

        StrandPublisher strandPublisher = new StrandPublisherImpl();

        StrandConversionTaskManager strandConversionManager = new StrandConversionTaskManager(
                4,
                strandPublisher,
                config.getQuantizers(),
                config.getTemporalGranularity());

        DataConnector dataConnector = new FileDataConnector(fileParser, files);

        strandConversionManager.start();
        dataConnector.initWithIngestionConfigAndRecordCallBackHandler(config, strandConversionManager);
        dataConnector.start();
        strandConversionManager.awaitCompletion();
        dataConnector.terminate();
    }

}
