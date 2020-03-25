package sustain.synopsis.samples.client.noaa;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;

import java.io.*;
import java.time.Duration;
import java.time.Instant;


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

        File[] inputFiles = new File[args.length-4];
        for (int i = 4; i < args.length; i++) {
            inputFiles[i-4] = new File(args[i]);
        }

        SessionSchema sessionSchema = new SessionSchema(
                Util.quantizerMapFromFile(binConfig),
                GEOHASH_LENGTH,
                TEMPORAL_BRACKET_LENGTH
        );

        StrandPublisher strandPublisher = new SimpleStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new ConsoleStrandPublisher();

        StrandRegistry strandRegistry = new StrandRegistry(strandPublisher, 10000, 100);

        NoaaIngester noaaIngester = new NoaaIngester(inputFiles, sessionSchema);


        long timeStart = Instant.now().toEpochMilli();
        while (noaaIngester.hasNext()) {
            Strand strand = noaaIngester.next();
            if (strand != null) {
                strandRegistry.add(strand);
            }
        }
        long totalStrandsPublished = strandRegistry.terminateSession();
        long timeEnd = Instant.now().toEpochMilli();
        double secondsElapsed = (timeEnd-timeStart) / 1000d;
        double strandsPerSec = totalStrandsPublished / secondsElapsed;

        System.out.println("Total Strands Published: " + totalStrandsPublished);
        System.out.printf("In Seconds: %.2f\n", secondsElapsed);
        System.out.printf("Strands per second: %.1f", strandsPerSec);
    }

}
