package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.samples.client.noaa.NoaaIngester;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class StreamFlowClient {

    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    public static void main(String[] args) throws IOException, CsvValidationException {
//        if (args.length < 5) {
//            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigPath inputDir");
//            return;
//        }
//
//        String dhtNodeAddress = args[0];
//        String datasetId = args[1];
//        long sessionId = Long.parseLong(args[2]);
//        String binConfig = args[3];
//
//        File baseDir = new File(args[4]);
//        File[] inputFiles = baseDir.listFiles(pathname -> pathname.getName().startsWith("stream_flow_co") && pathname.getName().endsWith(".gz"));
//
//        if (inputFiles == null) {
//            System.err.println("No matching files.");
//            return;
//        }
//
//        System.out.println("Total matching file count: " + inputFiles.length);
//        Arrays.sort(inputFiles, (o1, o2) -> o1.getName().compareTo(o2.getName()));

        Map<String, StationParser.Location> stationMap = StationParser.parseFile(new File("/Users/keegan/Downloads/station.csv"));
        System.out.println(stationMap.size());

//        SessionSchema sessionSchema = new SessionSchema(Util.quantizerMapFromString(binConfig), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);
        SessionSchema sessionSchema = new SessionSchema(null, GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);

        StreamFlowFileParser streamFlowFileParser = new StreamFlowFileParser(stationMap);

        RecordCallbackHandler recordCallbackHandler = new RecordCallbackHandler() {

            Map<String, Integer> recordCount = new HashMap<>();

            @Override
            public boolean onRecordAvailability(Record record) {
                recordCount.merge(record.getGeohash(), 1, Integer::sum);
                return true;
            }

            @Override
            public void onTermination() {
                int total = 0;
                for (String geohash : recordCount.keySet()) {
                    int localCount = recordCount.get(geohash);
                    total += localCount;
                    System.out.printf("%s %d\n", geohash, localCount);
                }
                System.out.println(total);
            }
        };

        streamFlowFileParser.initWithSchemaAndHandler(sessionSchema, recordCallbackHandler);

//        streamFlowFileParser.parse(new File("/Users/keegan/Sustain/usgs-stream-flow-downloader/stream_flow_co_2020_02_01_2020_02_02"));
        streamFlowFileParser.parse(new File("/Users/keegan/Sustain/usgs-stream-flow-downloader/stream_flow_co_2019_01_02_2019_01_03.gz"));

//        streamFlowFileParser.parse(new File("/Users/keegan/Sustain/usgs-stream-flow-downloader/stream_flow_co_2020_02_01_2020_02_02"));

        recordCallbackHandler.onTermination();


//        StrandPublisher strandPublisher = new SimpleStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new ConsoleStrandPublisher();

//        StrandRegistry strandRegistry = new StrandRegistry(strandPublisher, 10000, 100);
//
//        NoaaIngester noaaIngester = new NoaaIngester(Arrays.copyOfRange(inputFiles, inputFiles.length / 2 * i, inputFiles.length / 2 * (i + 1)), sessionSchema);
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


}
