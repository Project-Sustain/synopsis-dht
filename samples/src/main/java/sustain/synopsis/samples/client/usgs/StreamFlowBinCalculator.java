package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.sketch.dataset.Quantizer;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class StreamFlowBinCalculator {


    private static Random random = new Random(1);
    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BUCKET_LENGTH = Duration.ofHours(6);

    public static void main(String[] args) throws ParseException, IOException, CsvValidationException {

        File baseDir = new File(args[0]);
        File stationsFile = new File(args[1]);
        Date beginDate = StreamFlowClient.dateFormat.parse(args[2]);
        Date endDate = StreamFlowClient.dateFormat.parse(args[3]);
        int beginYear = Integer.parseInt(args[2].substring(0,4));
        int endYear =  Integer.parseInt(args[3].substring(0,4));
        // proportion of records to be included
        int daysToSkip = Integer.parseInt((args[4]));
        double proportion = Double.parseDouble(args[5]);


        List<File> inputFiles = StreamFlowClient.getMatchingFiles(baseDir.listFiles(), beginDate, endDate, beginYear, endYear);
        inputFiles.sort(Comparator.comparing(File::getName));
        System.out.println("Total matching file count: " + inputFiles.size());


        Set<String> features = new HashSet<>();
        if (args[6].contains("t")) {
            features.add(StreamFlowClient.TEMPERATURE_FEATURE);
        }
        if (args[6].contains("d")) {
            features.add(StreamFlowClient.DISCHARGE_FEATURE);
        }

        Map<String, Quantizer> quantizerMap = new HashMap<>();
        for (String s : features) {
            quantizerMap.put(s, new Quantizer());
        }

        MyRecordCallbackHandler handler = new MyRecordCallbackHandler(features, proportion);
        StreamFlowFileParser fileParser = new StreamFlowFileParser(StationParser.parseFile(stationsFile));
        fileParser.initWithSchemaAndHandler(new SessionSchema(quantizerMap, GEOHASH_LENGTH, TEMPORAL_BUCKET_LENGTH), handler);

        for (int i = 0; i < inputFiles.size(); i += daysToSkip) {
            fileParser.parse(inputFiles.get(i));
        }

        List<Record> records = handler.getRecords();
        System.out.println(records.size());

        String binConfiguration = new BinCalculator().getBinConfiguration(handler.getRecords());
        System.out.println(binConfiguration);

    }

    private static class MyRecordCallbackHandler implements RecordCallbackHandler {

        private Set<String> requiredFeatures;
        private List<Record> records = new ArrayList<>();
        private double proportion;

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
