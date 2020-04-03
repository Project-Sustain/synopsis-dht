package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.geohash.GeoHash;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class StreamFlowFileParser implements FileParser {

    private SessionSchema schema;
    RecordCallbackHandler recordCallbackHandler;
    final Map<String, StationParser.Location> stationMap;




    public StreamFlowFileParser(Map<String, StationParser.Location> stationMap) {
        this.stationMap = stationMap;
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        this.schema = schema;
        this.recordCallbackHandler = handler;
    }

    @Override
    public void parse(File file) {
        try {
            BufferedReader br;
            if (file.getName().endsWith(".gz")) {
                GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(file));
                br = new BufferedReader(new InputStreamReader(gzipInputStream));
            } else {
                br = new BufferedReader(new FileReader(file));
            }

            parseHeader(br);

            while(parseSite(br));

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void parseHeader(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.matches("# [-]+")) {
                break;
            }
        }
        reader.readLine();
    }



    private boolean parseSite(BufferedReader reader) throws IOException {
        StationParser.Location location = null;
        Integer streamFlowIdx;
        String dataCode = null;

        String line = reader.readLine();
        if (line == null) {
            return false;
        }

        String stationId = line.substring("# Data provided for site ".length());
        location = stationMap.get(stationId);
        reader.readLine();

        while ((line = reader.readLine()).startsWith("#")) {
            if (line.endsWith("Discharge, cubic feet per second")) {
                String[] splits = line.split("\\s+");
                dataCode = splits[1] + "_" + splits[2];
            }
        }

        String[] headerSplits = line.split("\t");
        Map<String, Integer> headerMap = new HashMap<>(headerSplits.length);
        for (int i = 0; i < headerSplits.length; i++) {
            headerMap.put(headerSplits[i], i);
        }
        streamFlowIdx = headerMap.get(dataCode);
        reader.readLine();


        String geohash = null;
        if (location != null) {
            geohash = GeoHash.encode(location.latitude, location.longitude, schema.getGeohashLength());
        }

        SiteDataParser siteDataParser = new SiteDataParser(headerMap, geohash, dataCode, recordCallbackHandler);
        return siteDataParser.parseSiteData(reader);
    }

}
