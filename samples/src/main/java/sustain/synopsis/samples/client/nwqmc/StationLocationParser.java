package sustain.synopsis.samples.client.nwqmc;

import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.connectors.file.CsvFileParser;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StationLocationParser {

    public static Map<String,Location> locationsForFile(File file) {
        Map<String,Location> ret = new HashMap<>();

        try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
            CSVReaderHeaderAware csvReaderHeaderAware = new CSVReaderHeaderAware(bf);

            String[] res;
            while ((res = csvReaderHeaderAware.readNext("MonitoringLocationIdentifier", "LatitudeMeasure", "LongitudeMeasure")) != null) {
                ret.put(res[0], new Location(Float.parseFloat(res[1]),Float.parseFloat(res[2])));
            }
        } catch (IOException | CsvValidationException e) { }
        return ret;
    }


}
