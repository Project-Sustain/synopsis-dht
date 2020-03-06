package synopsis2.samples.noaa;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.StrandRegistry;
import sustain.synopsis.sketch.dataset.Quantizer;
import synopsis2.client.IngestionConfig;
import synopsis2.client.Util;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

public class Driver {
    public static final String[] FEATURE_NAMES = {
            //"vegitation_type_as_in_sib_surface",
            //"vegetation_surface",
            "precipitable_water_entire_atmosphere",
            //"albedo_surface",
            "visibility_surface",
            //"pressure_surface",
            //"temperature_tropopause",
            "temperature_surface",
            "relative_humidity_zerodegc_isotherm", // 0-100 range
            //"downward_long_wave_rad_flux_surface",
            //"upward_short_wave_rad_flux_surface",
            //"snow_depth_surface",
            //"lightning_surface", // boolean
            //"ice_cover_ice1_no_ice0_surface", // boolean
            //"categorical_snow_yes1_no0_surface", // boolean
    };

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Missing input args. Usage: input_dir bin_config");
            return;
        }
        //String config = args[0];
        //String topic = args[1];
        String inputDir = args[0];
        String binConfig = args[1];
        System.out.println("Input dir: " + inputDir + ", Bin Config: " + binConfig);
        try {
            // initialize the context
            /*Properties initProps = new Properties();
            initProps.load(new FileReader(config));
            Context context = Context.getInstance();
            context.initialize(initProps);

            Util.createKafkaTopicIfNotExists(topic, 2, (short)3);
            Context ctxt = Context.getInstance();
            Properties configProperties = new Properties();
            configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ctxt.getProperty(ServerConstants.Configuration.KAFKA_BOOTSTRAP_BROKERS));
            configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
            configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
            Publisher<String, byte[]> publisher = new Publisher<>(configProperties);
             */
            Map<String, Quantizer> quantizerMap = Util.quantizerMapFromFile(binConfig);
            IngestionConfig ingestionConfig = new IngestionConfig(quantizerMap, 3,
                    Duration.ofHours(6));
            NOAAIngester ingester = new NOAAIngester(inputDir, ingestionConfig);
            StrandRegistry registry = new StrandRegistry(s -> {});
            while (ingester.hasNext()) {
                Strand strand = ingester.next();
                if (strand != null) {
                    registry.add(strand);
                }
            }
            long totalStrandsPublished = registry.terminateSession();
            System.out.println("Total Strands Published: " + totalStrandsPublished);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
