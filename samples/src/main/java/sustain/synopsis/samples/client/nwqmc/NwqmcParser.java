package sustain.synopsis.samples.client.nwqmc;

import sustain.synopsis.ingestion.client.connectors.RecordParseException;
import sustain.synopsis.ingestion.client.connectors.file.LineHandler;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;

import java.io.File;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

public class NwqmcParser implements LineHandler {

    Map<String, Location> stationLocationMap;
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-dd-mm hh:mm:ss");
    RecordTypeParser recordTypeParser;
    Map<String, CsvRecordType> csvRecordTypeMap;

    public NwqmcParser() {
        stationLocationMap = StationLocationParser.locationsForFile(new File("/Users/keegan/Sustain/etc/data/nwqmc_stations.csv"));
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        Set<String> features = schema.getFeatures();
        RecordTypeParser.RecordTypeParserBuilder builder = RecordTypeParser.newBuilder()
                .addRecordType("temperature", CsvRecordType.newBuilder()
                        .columnMustMatch("ResultMeasureValue", "\\d+[.]\\d")
                        .columnMustMatch("CharacteristicName","Temperature, water")
                        .build())
                .addRecordType("", CsvRecordType.newBuilder()
                        .columnMustMatch("ResultMeasureValue", "\\d+[.]\\d")
                        .columnMustMatch("CharacteristicName","Temperature, water")
                        .build())

        }
        if (features.contains(""))

    }

    @Override
    public void onDataAvailability(Map<String, Integer> columnMap, String[] splits) {




    }


    private long getEpochSecondsForCurrentLine() throws RecordParseException {
        String date = csvFileParser.getColumn("ActivityStartDate");
        String time = csvFileParser.getColumn("ActivityStartTime");
        LocalDateTime ldt = LocalDateTime.parse(date+" "+time);

        String timeZone = csvFileParser.getColumn("ActivityStartTime/TimeZoneCode");
        ZoneId zoneId = ZoneId.of(timeZone);

        ZonedDateTime zdt = ZonedDateTime.of(ldt, zoneId);

        return zdt.toEpochSecond();
    }

    private void main_parser(Record record, String[] splits) throws RecordParseException {
        long epochSeconds = getEpochSecondsForCurrentLine();
        if (features.contains("temperature")) {

        }

    }

    private boolean temperatureParser(Record record) {

        csvFileParser.getColumnOrNull("CharacteristicName");

    }

}
