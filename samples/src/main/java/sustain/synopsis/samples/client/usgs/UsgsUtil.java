package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.ingestion.client.core.BinCalculator;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.samples.client.common.Location;
import sustain.synopsis.samples.client.common.Util;
import sustain.synopsis.sketch.dataset.Quantizer;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

public class UsgsUtil {

    static void getStationIdHelper(File file, Set<String> set) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {

            boolean inSiteData = false;
            while (br.ready()) {
                String line = br.readLine();


                if (inSiteData) {
                    String prefix2 = "# -";
                    if (line.length() >= prefix2.length() && line.startsWith(prefix2)) {
                        break;
                    }

                    String[] splits = line.split(" +", 4);
                    String org = splits[1];
                    String id = splits[2];

                    String key = org+"-"+id;
                    set.add(key);

                } else {
                    String prefix = "# Data for the following";
                    if (line.length() >= prefix.length() && line.startsWith(prefix)) {
                        inSiteData = true;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    static List<String> getAllStationIds(List<File> files) {
        Set<String> idSet = new HashSet<>();
        int finished = 0;
        for (File f : files) {
            getStationIdHelper(f, idSet);
            if (++finished % 100 == 0) {
                System.out.println(finished+" "+f.getName()+" "+idSet.size());
            }
        }

        List<String> list = new ArrayList<>(idSet);
        list.sort(Comparator.naturalOrder());

        return list;
    }

    static void writeStationIds(String[] args) {
        File inputDir = new File(args[0]);
        File outFile = new File(args[1]);

        List<File> files = Util.getFilesRecursive(inputDir, 0);
        System.out.println("File list size: "+files.size());

        List<String> allStationIds = getAllStationIds(files);
        System.out.println("Complete");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))) {
            for (String id : allStationIds) {
                bw.write(id+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void writeSample(String[] args) throws IOException {
        File inputFile = new File(args[0]);
        File locationMapFile = new File(args[1]);
        File outputFile = new File(args[2]);

        Map<String, Location> locationMap = StationParser.parseFile(locationMapFile);
        Map<String, Quantizer> quantizerMap = new HashMap<>();
        quantizerMap.put(StreamFlowClient.DISCHARGE_FEATURE, new Quantizer());

        Set<String> features = new HashSet<>();
        features.add(StreamFlowClient.DISCHARGE_FEATURE);
        StreamFlowBinCalculator.MyRecordCallbackHandler handler = new StreamFlowBinCalculator.MyRecordCallbackHandler( features, 1d/20);
//        MyRecordCallbackHandler handler = new MyRecordCallbackHandler();
        StreamFlowParser streamFlowParser = new StreamFlowParser(locationMap);
        streamFlowParser.initWithSchemaAndHandler(new SessionSchema(quantizerMap, StreamFlowClient.GEOHASH_LENGTH, StreamFlowClient.TEMPORAL_BRACKET_LENGTH), handler);

        streamFlowParser.parse(new GZIPInputStream(new FileInputStream(inputFile)));
        System.out.println("records size: "+handler.records.size());
        List<Float> floats = handler.records.stream()
                .map(record -> record.getFeatures().get(StreamFlowClient.DISCHARGE_FEATURE))
                .collect(Collectors.toList());
        floats.sort(Comparator.naturalOrder());
        System.out.printf("min %f max %f\n", floats.get(0), floats.get(floats.size()-1));

        BinCalculator binCalculator = BinCalculator.newBuilder()
                .setTypePreference(BinCalculator.BinConfigTypePreference.OKDE)
                .setMaxTicks(20)
                .build();

        BinCalculator.BinCalculatorResult binCalculatorResult = binCalculator.calculateBins(handler.getRecords());
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile))) {
            bw.write(inputFile.getName()+"\n");
            bw.write(binCalculatorResult.toString());
            for (Float f : floats) {
                bw.write(f+"\n");
            }
        }
    }

    static class MyRecordCallbackHandler implements RecordCallbackHandler {

        List<Record> records = new ArrayList<>();
        @Override
        public boolean onRecordAvailability(Record record) {
            records.add(record);
            return true;
        }

        @Override
        public void onTermination() {

        }
    }


    static class FilePredicate implements Predicate<File> {

        final String state;
        final int yearStart;
        final int yearEnd;

        public FilePredicate(String state, int yearStart, int yearEnd) {
            this.state = state;
            this.yearStart = yearStart;
            this.yearEnd = yearEnd;
        }

        @Override
        public boolean test(File file) {
            int year = Integer.parseInt(file.getName().substring(3, 7));
            boolean yearMatches = year >= yearStart && year <= yearEnd;
            boolean stateMatches = file.getName().startsWith(state);

            return stateMatches && yearMatches;
        }
    }

    public static void main(String[] args) throws IOException {
        writeSample(args);
    }
}
