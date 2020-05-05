package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.core.BinCalculator;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.sketch.dataset.Quantizer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.*;

import static sustain.synopsis.samples.client.usgs.StreamFlowClient2.parseBackupDirectoryForMatchingFilePaths;

public class StreamFlowBinCalculator3 {

    static Logger logger = Logger.getLogger(StreamFlowBinCalculator3.class);
    private static Random random = new Random(1);
    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BUCKET_LENGTH = Duration.ofHours(6);

    public static void main(String[] args) throws ParseException, IOException, CsvValidationException {

        File backupDir = new File(args[0]);
        File stationsFile = new File(args[1]);
        StreamFlowClient2.state = args[2];
        StreamFlowClient2.beginDate = LocalDate.parse(args[3], StreamFlowClient2.dateFormatter);
        StreamFlowClient2.endDate = LocalDate.parse(args[4], StreamFlowClient2.dateFormatter);

        // proportion of records to be included
        int daysToSkip = Integer.parseInt((args[5]));
        double proportion = Double.parseDouble(args[6]);
        String outFilePath = args[8];

        List<StreamFlowClient2.RemoteFile> remoteFiles = parseBackupDirectoryForMatchingFilePaths(backupDir);
        remoteFiles.sort(Comparator.comparing(StreamFlowClient2.RemoteFile::getId));
        System.out.println("Total matching file count: " + remoteFiles.size());


        Set<String> features = new HashSet<>();
        if (args[7].contains("t")) {
            features.add(StreamFlowClient.TEMPERATURE_FEATURE);
        }
        if (args[7].contains("d")) {
            features.add(StreamFlowClient.DISCHARGE_FEATURE);
        }

        Map<String, Quantizer> quantizerMap = new HashMap<>();
        for (String s : features) {
            quantizerMap.put(s, new Quantizer());
        }

        MyRecordCallbackHandler handler = new MyRecordCallbackHandler(features, proportion);
        StreamFlowParser streamFlowParser = new StreamFlowParser(StationParser.parseFile(stationsFile));
        streamFlowParser.initWithSchemaAndHandler(new SessionSchema(quantizerMap, GEOHASH_LENGTH, TEMPORAL_BUCKET_LENGTH), handler);

        for (int i = 0; i < remoteFiles.size(); i += daysToSkip) {
            streamFlowParser.parse(remoteFiles.get(i).getInputStream());
        }

        List<Record> records = handler.getRecords();
        System.out.println(records.size());

        writeRecordsToFile(outFilePath, records);

//        String binConfiguration = new BinCalculator().getBinConfiguration(handler.getRecords());
//        System.out.println(binConfiguration);
    }

    static void writeRecordsToFile(String filePath, List<Record> records) {
        if (records.size() == 0) {
            return;
        }

        List<String> features = new ArrayList<>(records.get(0).getFeatures().keySet());
        features.sort(Comparator.naturalOrder());

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(new File(filePath)))) {
            for (String feature : features) {
                bw.write(feature+",");
            }
            bw.newLine();

            for (Record record : records) {
                for (String feature : features) {
                    Float value = record.getFeatures().get(feature);
                    String rep = value == null ? "null" : value.toString();
                    bw.write(rep+",");
                }
                bw.newLine();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
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
