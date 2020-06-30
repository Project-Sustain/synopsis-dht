package sustain.synopsis.samples.client.usgs;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.ingestion.client.publishing.DHTStrandPublisher;
import sustain.synopsis.metadata.*;
import sustain.synopsis.metadata.MetadataServiceGrpc.MetadataServiceBlockingStub;
import sustain.synopsis.samples.client.common.Location;
import sustain.synopsis.samples.client.common.State;
import sustain.synopsis.sketch.dataset.Quantizer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class StreamFlowClient {

    static Logger logger = Logger.getLogger(StreamFlowClient.class);
    public static final String TEMPERATURE_FEATURE = "temperature_water_degrees_celsius";
    public static final String DISCHARGE_FEATURE = "discharge_cubic_feet_per_second";
    public static final String GAGE_HEIGHT_FEATURE = "gage_height_feet";
    public static final String SPECIFIC_CONDUCTANCE_FEATURE = "specific_conductance_water_unfiltered_microsiemens_per_centimeter_at_25_degrees_celsius";

    public static final int GEOHASH_LENGTH = 6;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    static Map<String, String> binConfigMap = new HashMap<>();
    static String dhtNodeAddress;
    static String datasetId;
    static Map<String, Location> stationMap;
    static MetadataServiceBlockingStub metadataStub;

    static void ingest(List<File> allFiles, String stateAbbr, int year, long sessionId) {
        String key = stateAbbr+year;
        String binConfig = binConfigMap.get(key);
        logger.info(key);

        if (binConfig == null) {
            logger.info("skipping "+key+", bin config null");
            return;
        }

        List<File> files = allFiles.stream()
                .filter(new UsgsUtil.FilePredicate(stateAbbr, year, year))
                .sorted().collect(Collectors.toList());

        Map<String, Quantizer> quantizerMap = Util.quantizerMapFromString(binConfig);

        SessionSchema sessionSchema = new SessionSchema(quantizerMap, GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);
        DHTStrandPublisher publisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);

        PublishMetadataResponse publishMetadataResponse = metadataStub.publishMetadata(PublishMetadataRequest.newBuilder()
                .setDatasetId(datasetId)
                .setSessionMetadata(ProtoBuffSerializedSessionMetadata.newBuilder()
                        .setSessionId(sessionId)
                        .addBinConfiguration(serializedBinConfiguration(DISCHARGE_FEATURE, quantizerMap.get(DISCHARGE_FEATURE)))
                        .build()
                ).build()
        );

        logger.info("publish metadata response for sessionId "+sessionId+": "+publishMetadataResponse.getStatus());

        StrandRegistry strandRegistry = new StrandRegistry(publisher, 10000, 100);
        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(TEMPORAL_BRACKET_LENGTH);
        StrandConverterRecordCallBackHandler handler = new StrandConverterRecordCallBackHandler(strandRegistry, sessionSchema, temporalQuantizer);
        StreamFlowParser streamFlowParser = new StreamFlowParser(stationMap);
        streamFlowParser.initWithSchemaAndHandler(sessionSchema, handler);

        for (File f : files) {
            try {
                streamFlowParser.parse(new GZIPInputStream(new FileInputStream(f)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        strandRegistry.terminateSession();
        handler.onTermination();

        logger.info("Total strands: "+publisher.getStrandsPublishedCount());
    }

    static ProtoBuffSerializedBinConfiguration serializedBinConfiguration(String featureName, Quantizer binConfiguration) {
        return ProtoBuffSerializedBinConfiguration.newBuilder()
                .setFeatureName(featureName)
                .addAllValues(binConfiguration.getTicks().stream().map(f -> f.getFloat()).collect(Collectors.toList()))
                .build();
    }

    static Map<String, String> getBinConfigMap(File f) throws IOException {
        Map<String, String> ret = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            br.lines().forEach(line -> {
                String[] splits = line.split("\t");
                if (splits.length < 2) {
                    ret.put(line, null);
                } else  {
                    ret.put(splits[0], splits[1]);
                }
            });
        }
        return ret;
    }

    static MetadataServiceBlockingStub getMetadataStubForAddress(String metadataAddress) {
        String[] splits  = metadataAddress.split(":");
        String host = splits[0];
        int port = Integer.parseInt(splits[1]);

        Channel channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().build();
        MetadataServiceBlockingStub stub = MetadataServiceGrpc.newBlockingStub(channel);
        return stub;
    }

    public static void main(String[] args) throws IOException {
        File inputDir = new File(args[0]);
        stationMap = StationParser.parseFile(new File(args[1]));
        binConfigMap = getBinConfigMap(new File(args[2]));
        dhtNodeAddress = args[3];
        metadataStub = getMetadataStubForAddress(args[4]);
        datasetId = args[5];
        int beginYear = Integer.parseInt(args[6].substring(0,4));
        int endYear =  Integer.parseInt(args[7].substring(0,4));
        List<File> allFiles = sustain.synopsis.samples.client.common.Util.getFilesRecursive(inputDir, 0);

        long sessionId = 0;
        for (State s : State.values()) {
            String stateAbbr = s.getANSIAbbreviation().toLowerCase();
            for (int year = beginYear; year <= endYear; year++) {
                try {
                    ingest(allFiles, stateAbbr, year, sessionId++);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
