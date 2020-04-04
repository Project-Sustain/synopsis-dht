package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.samples.client.noaa.NoaaIngester;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamFlowClient {

    public static final String DISCHARGE_FEATURE = "discharge_cubic_feet_per_second";
    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    static Pattern datePattern = Pattern.compile("\\d\\d\\d\\d_\\d\\d_\\d\\d");
    public static boolean isFileInDateRange(String fileName, Date beginDate, Date endDate) {
        Matcher matcher = datePattern.matcher(fileName);
        try {
            if (!matcher.find()) {
                return false;
            }
            Date date1 = dateFormat.parse(matcher.group());
            if (!matcher.find()) {
                return false;
            }
            Date date2 = dateFormat.parse(matcher.group());
            return date1.compareTo(beginDate) >= 0 && date2.compareTo(endDate) <= 0;

        } catch (ParseException e) {
            return false;
        }
    }


    public static void main(String[] args) throws IOException, CsvValidationException, ParseException {
        if (args.length < 7) {
            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigFile stationLocationFile baseDir beginDate endDate");
            System.out.println("Dates in format yyyy_MM_dd");
            return;
        }

        String dhtNodeAddress = args[0];
        String datasetId = args[1];
        long sessionId = Long.parseLong(args[2]);
        String binConfigPath = args[3];
        File stationLocationFile = new File(args[4]);
        File baseDir = new File(args[5]);
        Date beginDate = dateFormat.parse(args[6]);
        Date endDate = dateFormat.parse(args[7]);


        File[] inputFiles = baseDir.listFiles(pathname -> pathname.getName().startsWith("stream_flow_co") && pathname.getName().endsWith(".gz"));
        System.out.println("Total matching file count: " + inputFiles.length);
        Arrays.sort(inputFiles, Comparator.comparing(File::getName));

        Map<String, StationParser.Location> stationMap = StationParser.parseFile(stationLocationFile);
        System.out.println("Num stations in stationLocationFile: " + stationMap.size());
        SessionSchema sessionSchema = new SessionSchema(Util.quantizerMapFromFile(binConfigPath), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);

        StrandPublisher strandPublisher = new SimpleStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
        StrandRegistry strandRegistry = new StrandRegistry(strandPublisher, 10000, 100);

        RecordCallbackHandler recordCallbackHandler = new StreamFlowRecordCallbackHandler(strandRegistry, sessionSchema);

        StreamFlowFileParser streamFlowFileParser = new StreamFlowFileParser(stationMap);
        streamFlowFileParser.initWithSchemaAndHandler(sessionSchema, recordCallbackHandler);

//        streamFlowFileParser.parse(new File("/Users/keegan/Sustain/usgs-stream-flow-downloader/stream_flow_co_2020_02_01_2020_02_02"));
        streamFlowFileParser.parse(new File("/Users/keegan/Sustain/usgs-stream-flow-downloader/stream_flow_co_2019_01_02_2019_01_03.gz"));

//        streamFlowFileParser.parse(new File("/Users/keegan/Sustain/usgs-stream-flow-downloader/stream_flow_co_2020_02_01_2020_02_02"));

        recordCallbackHandler.onTermination();

//
//        long timeStart = Instant.now().toEpochMilli();
//        while (noaaIngester.hasNext()) {
//            Strand strand = noaaIngester.next();
//            if (strand != null) {
//                strandRegistry.add(strand);
//            }
//        }
//        long totalStrandsPublished = strandRegistry.terminateSession();
//        long timeEnd = Instant.now().toEpochMilli();
//        double secondsElapsed = (timeEnd - timeStart) / 1000d;
//        double strandsPerSec = totalStrandsPublished / secondsElapsed;
//
//        System.out.println("Total Strands Published: " + totalStrandsPublished);
//        System.out.printf("In Seconds: %.2f\n", secondsElapsed);
//        System.out.printf("Strands per second: %.1f", strandsPerSec);

    }

    //        RecordCallbackHandler recordCallbackHandler = new RecordCallbackHandler() {
//            Map<String, Integer> recordCount = new HashMap<>();
//            @Override
//            public boolean onRecordAvailability(Record record) {
//                recordCount.merge(record.getGeohash(), 1, Integer::sum);
//                return true;
//            }
//
//            @Override
//            public void onTermination() {
//                int total = 0;
//                for (String geohash : recordCount.keySet()) {
//                    int localCount = recordCount.get(geohash);
//                    total += localCount;
//                    System.out.printf("%s %d\n", geohash, localCount);
//                }
//                System.out.println(total);
//            }
//        };


}
