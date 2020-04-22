package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import sustain.fileshare.FileshareServiceGrpc;
import sustain.fileshare.FileshareServiceGrpc.FileshareServiceBlockingStub;
import sustain.fileshare.FileshareServiceOuterClass;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.ingestion.client.publishing.SimpleStrandPublisher;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamFlowClient2 {

    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    static int fileshareServicePort = 44044;
    static Map<String, FileshareServiceBlockingStub> stubMap = new HashMap<>();

    static FileshareServiceBlockingStub createStubForHost(String host) {
        Channel channel = ManagedChannelBuilder
                .forAddress(host, fileshareServicePort)
                .usePlaintext()
                .build();

        return FileshareServiceGrpc.newBlockingStub(channel);
    }

    static FileshareServiceBlockingStub getStubForHost(String host) {
        FileshareServiceBlockingStub stub = stubMap.get(host);
        if (stub == null) {
            stub = createStubForHost(host);
            stubMap.put(host, stub);
        }
        return stub;
    }

    // example command line args:
    // localhost:9099
    // stream-flow-dataset
    // 1
    // /s/lattice-1/b/nobackup/galileo/stream-flow-data/stream_flow_bin_configuration.csv
    // /s/lattice-1/b/nobackup/galileo/stream-flow-data/stream_flow_co_stations.csv
    // /s/lattice-1/b/nobackup/galileo/stream-flow-data/co
    // 2016_01_01
    // 2020_01_01

    public static void addMatchingPaths(File f, String state, List<RemoteFile> listOut) {
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (line.startsWith(state)) {
                    String[] parts = line.split("[: ]");
                    listOut.add(new RemoteFile(parts[0], parts[1], parts[2]));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<RemoteFile> parseBackupDirectoryForMatchingFilePaths(File backupDir, String state) {
        List<RemoteFile> list = new ArrayList<>();
        for (File f : backupDir.listFiles()) {
            addMatchingPaths(f, state, list);
        }
        return list;
    }


    public static void main(String[] args) throws IOException, CsvValidationException, ParseException {
        if (args.length < 7) {
//            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigFile stationLocationFile baseDir beginDate endDate");
            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigFile stationLocationFile masterBackupDirectory state");

            System.out.println("Dates in format yyyy_MM_dd");
            return;
        }

        String dhtNodeAddress = args[0];
        String datasetId = args[1];
        long sessionId = Long.parseLong(args[2]);
        String binConfigPath = args[3];
        File stationLocationFile = new File(args[4]);
        File backupDir = new File(args[5]);
        String state = args[6];

        List<RemoteFile> remoteFiles = parseBackupDirectoryForMatchingFilePaths(backupDir, state);
        remoteFiles.sort(Comparator.comparing(RemoteFile::getId));


        System.out.println("Total matching file count: " + remoteFiles.size());

        Map<String, StationParser.Location> stationMap = StationParser.parseFile(stationLocationFile);
        System.out.println("Num stations in stationLocationFile: " + stationMap.size());
        SessionSchema sessionSchema = new SessionSchema(Util.quantizerMapFromFile(binConfigPath), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);

        SimpleStrandPublisher publisher = new SimpleStrandPublisher(dhtNodeAddress, datasetId, sessionId);
        StrandRegistry strandRegistry = new StrandRegistry(publisher, 10000, 100);

        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(TEMPORAL_BRACKET_LENGTH);
        StreamFlowRecordCallbackHandler handler = new StreamFlowRecordCallbackHandler(strandRegistry, sessionSchema, temporalQuantizer);

        StreamFlowParser streamFlowFileParser = new StreamFlowParser(stationMap);
        streamFlowFileParser.initWithSchemaAndHandler(sessionSchema, handler);

        for (RemoteFile rf : remoteFiles) {
            streamFlowFileParser.parse(rf.getInputStream());
        }

        strandRegistry.terminateSession();
        handler.onTermination();
        publisher.terminateSession();

        System.out.println("Total records: "+handler.totalRecordsHandled);
        System.out.println("Total strands: "+publisher.getTotalStrandsPublished());
        System.out.printf("Average records per strand: %.2f\n",(double)handler.totalRecordsHandled / publisher.getTotalStrandsPublished());
        System.out.println("Stations missing location data count: " + streamFlowFileParser.missingStationIds.size());
    }

    private static class RemoteFile {
        final String id;
        final String host;
        final String path;

        public RemoteFile(String id, String host, String path) {
            this.id = id;
            this.host = host;
            this.path = path;
        }

        public String getId() {
            return id;
        }

        public InputStream getInputStream() {
            FileshareServiceBlockingStub stubForHost = getStubForHost(host);
            FileshareServiceOuterClass.FileResponse fileResponse = stubForHost.requestFile(FileshareServiceOuterClass.FileRequest.newBuilder().setPath(path).build());
            return fileResponse.getData().newInput();
        }

    }




}
