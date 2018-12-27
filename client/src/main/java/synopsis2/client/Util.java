package synopsis2.client;

import io.sigpipe.sing.dataset.analysis.Quantizer;
import io.sigpipe.sing.dataset.feature.Feature;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Util {

    private static final Logger LOGGER = Logger.getLogger(Util.class);

    public static Map<String, Quantizer> quantizerMapFromFile(String filePath) throws IOException {
        Map<String, Quantizer> quantizers = new HashMap<>();
        try {
            FileReader fileReader = new FileReader(filePath);
            BufferedReader bfr = new BufferedReader(fileReader);
            String line;
            while ((line = bfr.readLine()) != null) {
                String[] segments = line.split(",");
                String fName = segments[0];
                Feature[] ticks = new Feature[segments.length - 4];
                for (int i = 4; i < segments.length; i++) {
                    ticks[i-4] = new Feature(Float.parseFloat(segments[i]));
                }
                Quantizer q = new Quantizer(ticks);
                quantizers.put(fName, q);
            }
        } catch (IOException e) {
            LOGGER.error("Error parsing the bin configuration.", e);
            throw e;
        }
        return quantizers;
    }
}
