package sustain.synopsis.samples.client.noaa;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.ingestion.client.publishing.SimpleStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.StrandPublisher;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class NoaaClient {

    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigPath inputFiles...");
            return;
        }

        String dhtNodeAddress = args[0];
        String datasetId = args[1];
        long sessionId = Long.parseLong(args[2]);
        String binConfig = args[3];

        File baseDir = new File(args[4]);
        File[] inputFiles = baseDir.listFiles(
                pathname -> pathname.getName().startsWith("namanl_218_201501") && pathname.getName().endsWith(
                        "001.grb" + ".mblob"));

        if (inputFiles == null) {
            System.err.println("No matching files.");
            return;
        }

        System.out.println("Total matching file count: " + inputFiles.length);

        // sort based on timestamps to reduce the temporally adjacent data being fragmented over multiple files
        // this provides better temporal locality during query evaluations.
        Arrays.sort(inputFiles, (o1, o2) -> {
            String[] splits1 = o1.getName().split("_");
            String[] splits2 = o2.getName().split("_");
            if (splits1[2].equals(splits2[2])) { // same day, sort based on time
                return Integer.compare(Integer.parseInt(splits1[3]), Integer.parseInt(splits2[3]));
            }
            return Integer.compare(Integer.parseInt(splits1[2]), Integer.parseInt(splits2[2])); // sort based on day
        });

        Arrays.stream(inputFiles).forEach(f -> {
            System.out.println(f.getName());
        });

        /*File[] inputFiles = new File[args.length-4];
        for (int i = 4; i < args.length; i++) {
            inputFiles[i-4] = new File(args[i]);
        }*/

        for (int i = 0; i < Math.min(2, inputFiles.length); i++) {   // ingest data over two sessions
            SessionSchema sessionSchema =
                    new SessionSchema(Util.quantizerMapFromFile(binConfig), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);

            StrandPublisher strandPublisher = new SimpleStrandPublisher(dhtNodeAddress, datasetId, sessionId + i);
//        StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new ConsoleStrandPublisher();

            StrandRegistry strandRegistry = new StrandRegistry(strandPublisher, 10000, 100);

            NoaaIngester noaaIngester = new NoaaIngester(
                    Arrays.copyOfRange(inputFiles, (int) Math.ceil(inputFiles.length / 2d) * i,
                                       (int) Math.ceil(inputFiles.length / 2d) * (i + 1)), sessionSchema);

            long timeStart = Instant.now().toEpochMilli();
            while (noaaIngester.hasNext()) {
                Strand strand = noaaIngester.next();
                if (strand != null) {
                    strandRegistry.add(strand);
                }
            }
            long totalStrandsPublished = strandRegistry.terminateSession();
            long timeEnd = Instant.now().toEpochMilli();
            double secondsElapsed = (timeEnd - timeStart) / 1000d;
            double strandsPerSec = totalStrandsPublished / secondsElapsed;

            System.out.println("Total Strands Published: " + totalStrandsPublished);
            System.out.printf("In Seconds: %.2f\n", secondsElapsed);
            System.out.printf("Strands per second: %.1f", strandsPerSec);
        }
    }
}
