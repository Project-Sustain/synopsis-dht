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
import java.util.HashMap;
import java.util.Map;

public class StreamFlowSiteDataParser {

    final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    final static Map<String, ZoneId> timeZoneMap = new HashMap<>();

    static {
        timeZoneMap.put("MST", ZoneId.of("America/Denver"));
    }

    final Map<String, Integer> headerMap;
    final String geohash;
    final String dataCode;
    final RecordCallbackHandler callbackHandler;

    public StreamFlowSiteDataParser(Map<String, Integer> headerMap, String geohash, String dataCode, RecordCallbackHandler callbackHandler) {
        this.headerMap = headerMap;
        this.geohash = geohash;
        this.dataCode = dataCode;
        this.callbackHandler = callbackHandler;
    }

    public void parseLine(String line) {
        String[] splits = line.split("\t");

        if (splits.length > headerMap.size() || headerMap.get(dataCode) == null) {
            return;
        }

        try {
            String datetime = splits[headerMap.get("datetime")];
            String timezone = splits[headerMap.get("tz_cd")];
            LocalDateTime localDateTime = LocalDateTime.parse(datetime, dateTimeFormatter);
            ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, timeZoneMap.get(timezone));

            long timestamp = zonedDateTime.toEpochSecond();
            float value = Float.parseFloat(splits[headerMap.get(dataCode)]);

            Record record = new Record();
            record.setGeohash(geohash);
            record.setTimestamp(timestamp);
            record.addFeatureValue(StreamFlowClient.DISCHARGE_FEATURE, value);
            callbackHandler.onRecordAvailability(record);

        } catch (Exception e) {

        }
    }

    public boolean parseSiteData(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                return true;
            }
            if (geohash == null || dataCode == null) {
                continue;
            }
            parseLine(line);
        }
        return false;
    }


}
