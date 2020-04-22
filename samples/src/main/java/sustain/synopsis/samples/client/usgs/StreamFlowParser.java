package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.geohash.GeoHash;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class StreamFlowParser {

    private SessionSchema schema;
    RecordCallbackHandler recordCallbackHandler;
    final Map<String, StationParser.Location> stationMap;

    public StreamFlowParser(Map<String, StationParser.Location> stationMap) {
        this.stationMap = stationMap;
    }

    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        this.schema = schema;
        this.recordCallbackHandler = handler;
    }

    public void parse(InputStream is) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(is)))) {

            parseHeader(br);
            StreamFlowSiteDataParser siteDataParser;
            while((siteDataParser = getSiteParser(br)) != null) {
                if (!siteDataParser.parseSiteData(br)) {
                    break;
                }
            }

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

    final HashSet<String> missingStationIds = new HashSet<>();

    private StreamFlowSiteDataParser getSiteParser(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            return null;
        }

        String stationId = line.substring("# Data provided for site ".length());
        StationParser.Location location = stationMap.get(stationId);
        if (location == null) {
            missingStationIds.add(stationId);
            return StreamFlowSiteDataParser.NO_OP_PARSER;
        }
        reader.readLine();

        Map<String, String>  dataCodesMap = new HashMap<>();
        while ((line = reader.readLine()).startsWith("#")) {
            String[] splits = line.split("\\s+");
            if (line.endsWith("Discharge, cubic feet per second")) {
                dataCodesMap.put(StreamFlowClient.DISCHARGE_FEATURE, splits[1] + "_" + splits[2]);
            } else if (line.endsWith("Temperature, water, degrees Celsius")) {
                dataCodesMap.put(StreamFlowClient.TEMPERATURE_FEATURE, splits[1] + "_" + splits[2]);
            } else if (line.endsWith("Gage height, feet")) {
                dataCodesMap.put(StreamFlowClient.GAGE_HEIGHT_FEATURE, splits[1] + "_" + splits[2]);
            } else if (line.endsWith("Specific conductance, water, unfiltered, microsiemens per centimeter at 25 degrees Celsius")) {
                dataCodesMap.put(StreamFlowClient.SPECIFIC_CONDUCTANCE_FEATURE, splits[1] + "_" + splits[2]);
            }
        }
        for (String s : schema.getFeatures()) {
            if (!dataCodesMap.containsKey(s)) {
                return StreamFlowSiteDataParser.NO_OP_PARSER;
            }
        }

        String[] headerSplits = line.split("\t");
        Map<String, Integer> headerMap = new HashMap<>(headerSplits.length);
        for (int i = 0; i < headerSplits.length; i++) {
            headerMap.put(headerSplits[i], i);
        }
        reader.readLine();

        return new StreamFlowSiteDataParser(
                headerMap,
                GeoHash.encode(location.latitude, location.longitude, schema.getGeohashLength()),
                dataCodesMap.values(),
                recordCallbackHandler
        );
    }

}
