package sustain.synopsis.ingestion.client.core;

import org.apache.log4j.Logger;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Util {

    private static final Logger LOGGER = Logger.getLogger(Util.class);

    public static Map<String, Quantizer> quantizerMapFromFile(String filePath) throws IOException {
        try {
            byte[] fileBytes = Files.readAllBytes(Paths.get(filePath));
            return quantizerMapFromString(new String(fileBytes, StandardCharsets.UTF_8));

        } catch (IOException e) {
            LOGGER.error("Error parsing the bin configuration.", e);
            throw e;
        }
    }

    public static Map<String, Quantizer> quantizerMapFromString(String s) {
        BufferedReader bfr = new BufferedReader(new StringReader(s));
        Map<String, Quantizer> quantizers = new HashMap<>();

        try {
            String line;
            while (true) {
                    if ((line = bfr.readLine()) == null) break;
                    String[] segments = line.split(",");
                    String fName = segments[0];
                    Feature[] ticks = new Feature[segments.length - 4];
                    for (int i = 4; i < segments.length; i++) {
                        ticks[i - 4] = new Feature(Float.parseFloat(segments[i]));
                    }
                    Quantizer q = new Quantizer(ticks);
                    quantizers.put(fName, q);
                }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return quantizers;
    }

    public static File[] getFilesFromStrings(int startIdx, String[] args) {
        if (startIdx >= args.length) {
            return new File[]{};
        }
        File[] files = new File[args.length-startIdx];
        for (int i = startIdx; i < args.length; i++) {
            files[i-startIdx] = (new File(args[i]));
        }
        return files;
    }


}
