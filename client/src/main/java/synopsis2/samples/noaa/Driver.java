package synopsis2.samples.noaa;

import io.sigpipe.sing.dataset.analysis.Quantizer;
import synopsis2.Strand;
import synopsis2.client.IngestionConfig;
import synopsis2.client.StrandRegistry;
import synopsis2.client.Util;
import synopsis2.client.kafka.Publisher;
import synopsis2.dht.Context;

import java.io.FileReader;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

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
        if (args.length < 4) {
            System.err.println("Missing input args. Usage: config topic input_dir bin_config");
            return;
        }
        String config = args[0];
        String topic = args[1];
        String inputDir = args[2];
        String binConfig = args[3];
        System.out.println("Input dir: " + inputDir + ", Bin Config: " + binConfig);
        try {
            // initialize the context
            Properties initProps = new Properties();
            initProps.load(new FileReader(config));
            Context context = Context.getInstance();
            context.initialize(initProps);

            Util.createKafkaTopicIfNotExists(topic, 2, (short)3);
            Publisher publisher = new Publisher();

            Map<String, Quantizer> quantizerMap = Util.quantizerMapFromFile(binConfig);
            IngestionConfig ingestionConfig = new IngestionConfig(Arrays.asList(FEATURE_NAMES), quantizerMap, 4, Duration.ofHours(6));
            NOAAIngester ingester = new NOAAIngester(inputDir, ingestionConfig);
            StrandRegistry registry = new StrandRegistry();
            while (ingester.hasNext()) {
                Strand strand = ingester.next();
                if (strand != null) {
                    int recordCount = registry.add(strand);
                    if (recordCount % 100 == 0) {
                        System.out.println(recordCount);
                        // todo: remove next two lines
                        registry.publish(topic, publisher);
                        break;
                    }
                    if (registry.isBatchReady()) {
                        registry.publish(topic, publisher);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
