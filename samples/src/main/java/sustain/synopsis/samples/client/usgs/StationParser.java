package sustain.synopsis.samples.client.usgs;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StationParser {

    public static Map<String, Location> parseFile(File file) throws IOException, CsvValidationException {
        CSVReader csvReader = new CSVReader(new FileReader(file));
        csvReader.skip(1);

        Map<String, Location> stationMap = new HashMap<>();
        String[] splits;
        while ((splits = csvReader.readNext()) != null) {
            String id = splits[2];

            if (!id.matches("USGS-\\d+")) {
                continue;
            }

            float lat = Float.parseFloat(splits[11]);
            float lng = Float.parseFloat(splits[12]);
            stationMap.put(id.substring(5), new Location(lat,lng));
        }
        return stationMap;
    }

    static class Location {
        float latitude;
        float longitude;

        public Location(float latitude, float longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

}
