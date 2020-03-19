package sustain.synopsis.samples.client.nwqmc;

import com.opencsv.CSVReaderHeaderAware;
import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.geohash.GeoHash;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StationLocationParser {

    public static Map<String, String> getGeohashMapFromFile(File file, int geohashPrecision) {
        Map<String,String>  stationGeohashMap = new HashMap<>();

        try (BufferedReader bf = new BufferedReader(new FileReader(file))) {
            CSVReaderHeaderAware csvReaderHeaderAware = new CSVReaderHeaderAware(bf);

            String[] line;
            while ((line = csvReaderHeaderAware.readNext("MonitoringLocationIdentifier", "LatitudeMeasure", "LongitudeMeasure")) != null) {
                float lat = Float.parseFloat(line[1]);
                float lng = Float.parseFloat(line[2]);
                String stationId = line[0];
                String geohash = GeoHash.encode(lat, lng, geohashPrecision);
                stationGeohashMap.put(stationId, geohash);
            }
        } catch (IOException | CsvValidationException e) {
            
        }

        return stationGeohashMap;
    }

}
