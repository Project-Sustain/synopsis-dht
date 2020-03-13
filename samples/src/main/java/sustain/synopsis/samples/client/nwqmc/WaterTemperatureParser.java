package sustain.synopsis.samples.client.nwqmc;

import sustain.synopsis.ingestion.client.connectors.RecordParseException;
import sustain.synopsis.ingestion.client.connectors.file.CsvFileParser;
import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.Util;
import sustain.synopsis.sketch.util.Geohash;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

public class WaterTemperatureParser implements FileParser {


    Map<String, Location> stationLocationMap;
    CsvFileParser csvFileParser;
    SessionSchema schema;

    public WaterTemperatureParser() {
        stationLocationMap = StationLocationParser.locationsForFile(new File("/Users/keegan/Sustain/etc/data/nwqmc_stations.csv"));
    }

    public static SessionSchema getHardCodedSessionSchema() {
        try {
            SessionSchema sessionSchema = new SessionSchema(
                    Util.quantizerMapFromFile("/Users/keegan/Sustain/etc/data/nwqmc_binconfig.txt"),
                    3,
                    Duration.ofDays(7));

            return sessionSchema;

        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        this.schema = schema;
        csvFileParser = CsvFileParser.newBuilder()
                .setReadHeader(true)
                .withFeatureParser(this::parse)
                .build();

        csvFileParser.initWithSchemaAndHandler(schema, handler);
    }

    private void parse(Record record, String[] splits) throws RecordParseException {
        String timeZone = splits[csvFileParser.getColumnIdx("ActivityStartTime/TimeZoneCode")];
        if ( !(timeZone.equals("MST") || timeZone.equals("MDT")) ) {
            throw new RecordParseException();
        }

        try {
            String startTime = splits[csvFileParser.getColumnIdx("ActivityStartTime/Time")];
            String[] hms = startTime.split(":");
            int hour = Integer.parseInt(hms[0]);
            int minutes = Integer.parseInt(hms[1]);
            int seconds = Integer.parseInt(hms[2]);
            String startDate = splits[csvFileParser.getColumnIdx("ActivityStartDate")];
            String[] ymd = startDate.split("-");
            int year = Integer.parseInt(ymd[0]);
            int month = Integer.parseInt(ymd[1]) - 1;
            int day = Integer.parseInt(ymd[2]);

            Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(timeZone));
            calendar.set(year, month, day, hour, minutes, seconds);
            record.setTimestamp(calendar.getTimeInMillis());

            String temperatureC = splits[csvFileParser.getColumnIdx("ResultMeasureValue")];
            float temperature = Float.parseFloat(temperatureC);
            if (temperature > 50) {
                throw new RecordParseException();
            }
            record.addFeatureValue("water-temperature", temperature);


            String stationId = splits[csvFileParser.getColumnIdx("MonitoringLocationIdentifier")];
            Location location = stationLocationMap.get(stationId);
            String geohash = Geohash.encode(location.lat,location.lng, schema.getGeohashLength());
            record.setGeohash(geohash);

        } catch (NumberFormatException e) {
            throw new RecordParseException();
        }
    }

    @Override
    public void parse(File file) {
        csvFileParser.parse(file);
    }

}
