package sustain.synopsis.samples.client.usgs;

import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.core.BinCalculator;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.samples.client.common.Location;
import sustain.synopsis.samples.client.common.State;
import sustain.synopsis.samples.client.common.Util;
import sustain.synopsis.sketch.dataset.Quantizer;

import java.io.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class StreamFlowBinCalculator {

    static Logger logger = Logger.getLogger(StreamFlowBinCalculator.class);
    private static Random random = new Random(1);
    public static final int GEOHASH_LENGTH = 6;
    public static final Duration TEMPORAL_BUCKET_LENGTH = Duration.ofHours(6);
    public static final BinCalculator binCalculator = BinCalculator.newBuilder()
            .setTypePreference(BinCalculator.BinConfigTypePreference.OKDE)
            .build();

    static File outputFile;
    static double proportion;
    static Set<String> features;
    static Map<String, Location> locationMap;
    static Map<String, Quantizer> quantizerMap;
//    static List<File> allFiles;
    static int yearBegin;
    static int yearEnd;

    static BinCalculator.BinCalculatorResult getBinConfiguration(List<File> allFiles, String stateAbbr, int yearStart, int yearEnd) {
        List<File> files = allFiles.stream()
                .filter(new UsgsUtil.FilePredicate(stateAbbr, yearStart, yearEnd))
                .sorted().collect(Collectors.toList());


        MyRecordCallbackHandler handler = new MyRecordCallbackHandler(features, proportion);
        StreamFlowParser streamFlowParser = new StreamFlowParser(locationMap);
        streamFlowParser.initWithSchemaAndHandler(new SessionSchema(quantizerMap, GEOHASH_LENGTH, TEMPORAL_BUCKET_LENGTH), handler);

        for (File f : files) {
            try {
                streamFlowParser.parse(new GZIPInputStream(new FileInputStream(f)));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

//        System.out.println("Record count: "+handler.getRecords().size());
        BinCalculator.BinCalculatorResult result = binCalculator.calculateBins(handler.getRecords());
        return result;
    }


    public static void main(String[] args) throws IOException {
        File inputDir = new File(args[0]);
        File stationsFile = new File(args[1]);
        outputFile = new File(args[2]);
        // proportion of records to be included
        int filesToSkip = Integer.parseInt((args[3]));
        proportion = Double.parseDouble(args[4]);
        String featureArg = args[5];
        yearBegin = Integer.parseInt(args[6]);
        yearEnd = Integer.parseInt(args[7]);

        features = new HashSet<>();
        if (featureArg.contains("t")) {
            features.add(StreamFlowClient.TEMPERATURE_FEATURE);
        }
        if (featureArg.contains("d")) {
            features.add(StreamFlowClient.DISCHARGE_FEATURE);
        }
        locationMap = StationParser.parseFile(stationsFile);

        quantizerMap = new HashMap<>();
        for (String s : features) {
            quantizerMap.put(s, new Quantizer());
        }

        List<File> allFiles = Util.getFilesRecursive(inputDir, filesToSkip);

        Map<String, BinCalculator.BinCalculatorResult> binConfigs = new HashMap<>();
        for (State state : State.values()) {
            String stateAbbr = state.getANSIAbbreviation().toLowerCase();

            for (int year = yearBegin; year <= yearEnd; year++) {
                String key = stateAbbr+year;
                BinCalculator.BinCalculatorResult binConfig = getBinConfiguration(allFiles, stateAbbr, year, year);
                binConfigs.put(key, binConfig);
                logger.info(key);
            }
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            for (String key : binConfigs.keySet().stream().sorted().collect(Collectors.toList())) {
                if (binConfigs.get(key) != null) {
                    bw.write(key+"\n");
                    bw.write(binConfigs.get(key)+"\n");
                }
            }
        }
    }

    static class MyRecordCallbackHandler implements RecordCallbackHandler {

        Set<String> requiredFeatures;
        List<Record> records = new ArrayList<>();
        final double proportion;

        public MyRecordCallbackHandler(Set<String> features, double proportion) {
            this.requiredFeatures = features;
            this.proportion = proportion;
        }

        @Override
        public boolean onRecordAvailability(Record record) {
            if (record.getFeatures().size() < requiredFeatures.size()) {
                return true;
            }
            Record copy = new Record();
            copy.setTimestamp(record.getTimestamp());
            copy.setGeohash(record.getGeohash());

            for (String featureKey : requiredFeatures) {
                Float value = record.getFeatures().get(featureKey);
                if (value == null) {
                    return true;
                }
                copy.addFeatureValue(featureKey, value);
            }

            if (random.nextDouble() < proportion) {
                records.add(copy);
            }
            return true;
        }

        @Override
        public void onTermination() {

        }

        public List<Record> getRecords() {
            return records;
        }

    }

}
