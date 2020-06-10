package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.geohash.GeoHash;

import java.io.BufferedReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StreamFlowSiteDataParser {

    final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    final static Map<String, ZoneId> timeZoneMap = new HashMap<>();

    static {
        timeZoneMap.put("PDT", ZoneId.of("US/Pacific-New"));
        timeZoneMap.put("PST", ZoneId.of("US/Pacific-New"));

        timeZoneMap.put("MDT", ZoneId.of("US/Mountain"));
        timeZoneMap.put("MST", ZoneId.of("US/Mountain"));

        timeZoneMap.put("CST", ZoneId.of("US/Central"));
        timeZoneMap.put("CDT", ZoneId.of("US/Central"));

        timeZoneMap.put("EST", ZoneId.of("US/Eastern"));
        timeZoneMap.put("EDT", ZoneId.of("US/Eastern"));
    }

    String geohash = null;
    Map<String, Integer> headerMap;
    Collection<String> dataCodes;
    RecordCallbackHandler callbackHandler;
    Map<String, StationParser.Location> locationMap;
    int geohashLength;
    boolean valid;
    long exceptionLineCount = 0;

    public static final StreamFlowSiteDataParser NO_OP_PARSER = new StreamFlowSiteDataParser();

    private StreamFlowSiteDataParser() {
        valid = false;
    }

    public StreamFlowSiteDataParser(Map<String, Integer> headerMap, Map<String, StationParser.Location> locationMap, Collection<String> dataCodes, RecordCallbackHandler callbackHandler, int geohashLength) {
        this.headerMap = headerMap;
        this.locationMap = locationMap;
        this.dataCodes = dataCodes;
        this.callbackHandler = callbackHandler;
        this.geohashLength = geohashLength;
        this.valid = true;
    }

    public void parseLine(String line) {
        String[] splits = line.split("\t");
        if (splits.length > headerMap.size()) {
            valid = false;
            return;
        }

        try {
            if (geohash == null) {
                String org = splits[0];
                String site_no = splits[1];
                String id = org+"-"+site_no;
                StationParser.Location location = locationMap.get(id);
                this.geohash = GeoHash.encode(location.latitude, location.longitude, geohashLength);
            }

            Record record = new Record();
            for (String dataCode : dataCodes) {
                if (dataCode.contains("00060")) {
                    float flow = Float.parseFloat(splits[headerMap.get(dataCode)]);
                    record.addFeatureValue(StreamFlowClient.DISCHARGE_FEATURE, flow);

                } else if (dataCode.contains("00010")) {
                    float temperature = Float.parseFloat(splits[headerMap.get(dataCode)]);
                    record.addFeatureValue(StreamFlowClient.TEMPERATURE_FEATURE, temperature);

                }
            }

            String datetime = splits[headerMap.get("datetime")];
            String timezone = splits[headerMap.get("tz_cd")];
            LocalDateTime localDateTime = LocalDateTime.parse(datetime, dateTimeFormatter);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, timeZoneMap.get(timezone));
            long timestamp = zonedDateTime.toEpochSecond()*1000;

            record.setGeohash(geohash);
            record.setTimestamp(timestamp);
            callbackHandler.onRecordAvailability(record);

        } catch (Exception e) {
            exceptionLineCount++;
        }
    }

    public boolean parseSiteData(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                return true;
            }
            if (valid) {
                parseLine(line);
            }
        }
        return false;
    }


}
