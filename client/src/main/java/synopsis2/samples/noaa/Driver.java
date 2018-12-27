package synopsis2.samples.noaa;

import io.sigpipe.sing.dataset.analysis.Quantizer;
import synopsis2.Strand;
import synopsis2.client.IngestionConfig;
import synopsis2.client.StrandRegistry;
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
        if(args.length < 2){
            System.err.println("Missing input args.");
            return;
        }
        String inputDir = args[0];
        String binConfig = args[1];
        System.out.println("Input dir: " + inputDir + ", Bin Config: " + binConfig);
        try {
            Map<String, Quantizer> quantizerMap = Util.quantizerMapFromFile(binConfig);
            IngestionConfig ingestionConfig = new IngestionConfig(Arrays.asList(FEATURE_NAMES), quantizerMap, 4, Duration.ofHours(6));
            NOAAIngester ingester = new NOAAIngester(inputDir, ingestionConfig);
            StrandRegistry registry = new StrandRegistry();
            while(ingester.hasNext()){
                Strand strand = ingester.next();
                if(strand != null){
                    int recordCount = registry.add(strand);
                    System.out.println(recordCount);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
