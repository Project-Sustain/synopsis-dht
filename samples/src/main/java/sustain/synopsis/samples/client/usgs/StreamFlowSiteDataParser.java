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
import java.util.List;
import java.util.Map;

public class StreamFlowSiteDataParser {

    final static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
    final static Map<String, ZoneId> timeZoneMap = new HashMap<>();

    static {
        timeZoneMap.put("MST", ZoneId.of("America/Denver"));
    }

    final Map<String, Integer> headerMap;
    final String geohash;
    final List<String> dataCodes;
    final RecordCallbackHandler callbackHandler;
    long exceptionLineCount = 0;

    public StreamFlowSiteDataParser(Map<String, Integer> headerMap, String geohash, List<String> dataCodes, RecordCallbackHandler callbackHandler) {
        this.headerMap = headerMap;
        this.geohash = geohash;
        this.dataCodes = dataCodes;
        this.callbackHandler = callbackHandler;
    }

    public void parseLine(String line) {
        String[] splits = line.split("\t");
        if (splits.length > headerMap.size()) {
            return;
        }

        try {
            Record record = new Record();
            for (String dataCode : dataCodes) {
                if (dataCode.contains("00060")) {
                    float flow = Float.parseFloat(splits[headerMap.get(dataCode)]);
                    record.addFeatureValue(StreamFlowClient.TEMPERATURE_FEATURE, flow);

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

        }
    }

    public boolean parseSiteData(BufferedReader reader) throws IOException {
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.startsWith("#")) {
                return true;
            }
            if (geohash == null) {
                continue;
            }
            parseLine(line);
        }
        return false;
    }


}
