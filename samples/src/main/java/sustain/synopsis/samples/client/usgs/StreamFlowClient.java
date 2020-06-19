package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import org.apache.log4j.Logger;
import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.ingestion.client.publishing.ConsoleStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.DHTStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.SimpleStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.StrandPublisher;
import sustain.synopsis.samples.client.common.Location;
import sustain.synopsis.samples.client.common.State;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

        SessionSchema sessionSchema = new SessionSchema(Util.quantizerMapFromString(binConfig), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);
//        DHTStrandPublisher publisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
        StrandPublisher publisher = new MyStrandPublisher();

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

        logger.info("Total strands: "+publisher.getTotalStrandsPublished());
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

    public static void main(String[] args) throws IOException {
        File inputDir = new File(args[0]);
        stationMap = StationParser.parseFile(new File(args[1]));
        binConfigMap = getBinConfigMap(new File(args[2]));
        dhtNodeAddress = args[3];
        datasetId = args[4];
        int beginYear = Integer.parseInt(args[5].substring(0,4));
        int endYear =  Integer.parseInt(args[6].substring(0,4));
        List<File> allFiles = sustain.synopsis.samples.client.common.Util.getFilesRecursive(inputDir, 0);


        long sessionId = 0;
        for (State s : State.values()) {
            String stateAbbr = s.getANSIAbbreviation().toLowerCase();
            for (int year = beginYear; year <= endYear; year++) {
                ingest(allFiles, stateAbbr, year, sessionId++);
            }
        }

    }

    static class MyStrandPublisher implements StrandPublisher {

        int count = 0;

        @Override
        public void publish(long messageId, Iterable<Strand> strands) {
            count++;
        }

        @Override
        public void terminateSession() {

        }

        @Override
        public long getTotalStrandsPublished() {
            return count;
        }

    }


}
