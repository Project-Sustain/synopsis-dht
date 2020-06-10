package sustain.synopsis.samples.client.usgs;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.geohash.GeoHash;
import sustain.synopsis.sketch.util.Geohash;

import java.io.*;
import java.util.*;

public class StationParser {

    static class Location {
        float latitude;
        float longitude;

        public Location(float latitude, float longitude) {
            this.latitude = latitude;
            this.longitude = longitude;
        }
    }

    public static Map<String, Location> parseFile(File file) throws IOException {
        Map<String, Location> stationMap = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while (br.ready()) {
                String[] splits = br.readLine().split(" ");
                String id = splits[0];
                Location l = new Location(Float.parseFloat(splits[1]), Float.parseFloat(splits[2]));
                stationMap.put(id, l);
            }
        }
        return stationMap;
    }

    private static void parseStationHelper(File f, Set<String> stationIds, Map<String, Location> locations) {
        try (CSVReader csvReader = new CSVReader(new FileReader(f))) {
            csvReader.skip(1);

            String[] splits;
            while ((splits = csvReader.readNext()) != null) {
                String id = splits[2];

                if (stationIds.contains(id)) {
                    float lat = Float.parseFloat(splits[11]);
                    float lng = Float.parseFloat(splits[12]);
                    locations.put(id, new Location(lat,lng));
                }
            }

        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    private static Map<String, Location> parseStationsFiles(List<File> files, Set<String> stationIds) {
        Map<String, Location> ret = new HashMap<>();
        for (File f : files) {
            if (f.getName().endsWith(".csv")) {
                parseStationHelper(f, stationIds, ret);
            }
        }
        return ret;
    }

    private static Set<String> getStationIds(File f) {
        Set<String> ret = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(f))) {
            while (br.ready()) {
                String line = br.readLine();
                ret.add(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return ret;
    }

    private static void writeToFile(File f, Map<String, Location> locationMap) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(f))) {
            List<String> keys = new ArrayList<>(locationMap.keySet());
            keys.sort(Comparator.naturalOrder());
            for (String key : keys) {
                Location l = locationMap.get(key);
                String geohash = GeoHash.encode(l.latitude, l.longitude, 6);
                bw.write(key+" "+l.latitude+" "+l.longitude+" "+geohash+"\n");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        File inputDir = new File(args[0]);
//        File allStationsFile = new File(args[1]);
//        File outputFile = new File(args[2]);
//
//        List<File> files = Util.getFilesRecursive(inputDir, 0);
//        Set<String> allStations = getStationIds(allStationsFile);
//        Map<String, Location> stringLocationMap = parseStationsFiles(files, allStations);
//        writeToFile(outputFile, stringLocationMap);
//    }

}
