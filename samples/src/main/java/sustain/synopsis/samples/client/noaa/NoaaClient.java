package sustain.synopsis.samples.client.noaa;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.ingestion.client.publishing.DHTStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.SimpleStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.StrandPublisher;
import sustain.synopsis.metadata.GetMetadataResponse;
import sustain.synopsis.metadata.ProtoBuffSerializedBinConfiguration;
import sustain.synopsis.metadata.ProtoBuffSerializedDatasetMetadata;
import sustain.synopsis.metadata.ProtoBuffSerializedSessionMetadata;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class NoaaClient {

    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    public static final GetMetadataResponse NOAA_EXAMPLE_METADATA_RESPONSE = GetMetadataResponse.newBuilder()
            .addDatasetMetadata(
                    ProtoBuffSerializedDatasetMetadata.newBuilder()
                            .setDatasetId("noaa-2014")
                            .addSessionMetadata(
                                    ProtoBuffSerializedSessionMetadata.newBuilder()
                                            .setSessionId(1)
                                            .addBinConfiguration(
                                            ProtoBuffSerializedBinConfiguration.newBuilder()
                                    .setFeatureName("precipitable_water_entire_atmosphere")
                                    .addValues((float) -8.34404193162917)
                                    .addValues((float)-1.2860326673303453)
                                    .addValues((float)5.771976596968479)
                                    .addValues((float)12.829985861267302)
                                    .addValues((float)19.88799512556613)
                                    .addValues((float)26.946004389864946)
                                    .addValues((float)34.00401365416377)
                                    .addValues((float)41.062022918462596)
                                    .addValues((float)48.12003218276142)
                                    .addValues((float)55.178041447060245)
                                    .addValues((float)62.23605071135906)
                                    .addValues((float)69.2940599756579)
                                    .addValues((float)76.35206923995672)
                                    .addValues((float)83.41007850425555)
                                    .addValues((float)90.46808776855437)
                                    .build()
                    )
                            .addBinConfiguration(
                                    ProtoBuffSerializedBinConfiguration.newBuilder()
                                            .setFeatureName("visibility_surface")
                                            .addValues((float) -1759.9194969172195)
                                            .addValues((float)1662.3107932972966)
                                            .addValues((float)5084.541083511813)
                                            .addValues((float)8506.77137372633)
                                            .addValues((float)11929.001663940846)
                                            .addValues((float)15351.231954155359)
                                            .addValues((float)18773.462244369875)
                                            .addValues((float)22195.692534584392)
                                            .addValues((float)25617.92282479891)
                                            .build()
                            )
                            .addBinConfiguration(
                                    ProtoBuffSerializedBinConfiguration.newBuilder()
                                            .setFeatureName("temperature_surface")
                                            .addValues((float) 217.90924072265673)
                                            .addValues((float)227.61221064715863)
                                            .addValues((float)237.3151805716605)
                                            .addValues((float)247.0181504961624)
                                            .addValues((float)256.7211204206643)
                                            .addValues((float)266.4240903451662)
                                            .addValues((float)276.12706026966805)
                                            .addValues((float)285.8300301941699)
                                            .addValues((float)295.53300011867185)
                                            .addValues((float)305.2359700431737)
                                            .addValues((float)314.9389399676756)
                                            .addValues((float)324.6419098921775)
                                            .addValues((float)334.3448798166794)
                                            .build()
                            )
                            .addBinConfiguration(
                                    ProtoBuffSerializedBinConfiguration.newBuilder()
                                            .setFeatureName("relative_humidity_zerodegc_isotherm")
                                            .addValues((float) -14.999999999999963)
                                            .addValues((float)-6.80112890625014)
                                            .addValues((float)1.3977421874996825)
                                            .addValues((float)9.596613281249505)
                                            .addValues((float)17.795484374999326)
                                            .addValues((float)25.99435546874915)
                                            .addValues((float)34.19322656249897)
                                            .addValues((float)42.392097656248794)
                                            .addValues((float)50.590968749998616)
                                            .addValues((float)58.789839843748446)
                                            .addValues((float)66.98871093749827)
                                            .addValues((float)75.18758203124808)
                                            .addValues((float)83.38645312499791)
                                            .addValues((float)91.58532421874774)
                                            .addValues((float)99.78419531249756)
                                            .addValues((float)107.98306640624737)
                                            .addValues((float)116.1819374999972)
                                            .build()
                            )
                                            .build()
                            )
                            .build()
            )
            .build();




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

            StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId + i);
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
