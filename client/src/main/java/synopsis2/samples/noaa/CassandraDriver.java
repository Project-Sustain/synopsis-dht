package synopsis2.samples.noaa;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.StrandRegistry;
import sustain.synopsis.sketch.dataset.Quantizer;
import synopsis2.client.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

public class CassandraDriver {
    public static final String[] FEATURE_NAMES = {
            "precipitable_water_entire_atmosphere",
            "visibility_surface",
            "temperature_surface",
            "relative_humidity_zerodegc_isotherm",
    };

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Missing input args. Usage: input_dir bin_config");
            return;
        }

        String inputDir = args[0];
        String binConfig = args[1];
        System.out.println("Input dir: " + inputDir + ", Bin Config: " + binConfig);
        try {

            Map<String, Quantizer> quantizerMap = Util.quantizerMapFromFile(binConfig);
            IngestionConfig ingestionConfig = new IngestionConfig(Arrays.asList(FEATURE_NAMES), quantizerMap, 3,
                    Duration.ofHours(6));
            NOAAIngester ingester = new NOAAIngester(inputDir, ingestionConfig);

            CassandraConnection connection = new CassandraConnection("127.0.0.1");
            CassandraIngestionConfig config = CassandraIngestionConfig.fromIngestConfigWithSessionId(ingestionConfig, 1);

            StrandRegistry registry = new StrandRegistry(new CassandraStrandPublisher(connection,config));
            while (ingester.hasNext()) {
                Strand strand = ingester.next();
                if (strand != null) {
                    int recordCount = registry.add(strand);
                    if(recordCount % 100 == 0){
                        System.out.println("Records processed: " + recordCount);
                    }
                }
            }
            int totalStrandsPublished = registry.terminateSession();
            System.out.println("Total Strands Published: " + totalStrandsPublished);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
