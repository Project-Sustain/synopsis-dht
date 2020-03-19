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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class NwqmcParser implements LineHandler {

    Map<String, String> stationGeohashMap;
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-dd-mm hh:mm:ss");
    RecordTypeParser recordTypeParser;
    Set<RecordType> requiredRecordTypes;

    public NwqmcParser() {
    }

    void initRecordTypeParser(Set<String> features) {
        RecordType[] recordTypes = new RecordType[]{
                RecordType.newBuilder()
                        .setId("Temperature, water")
                        .columnMustMatch("CharacteristicName","Temperature, water")
                        .build(),
                RecordType.newBuilder()
                        .setId("Stream flow, instantaneous")
                        .columnMustMatch("CharacteristicName","Stream flow, instantaneous")
                        .build(),
                RecordType.newBuilder()
                        .setId("RBP Stream width")
                        .columnMustMatch("CharacteristicName","RBP Stream width")
                        .build(),
                RecordType.newBuilder()
                        .setId("Specific conductance")
                        .columnMustMatch("CharacteristicName","Specific conductance")
                        .build(),
        };

        RecordTypeParser.RecordTypeParserBuilder recordTypeParserBuilder = RecordTypeParser.newBuilder();
        for (RecordType recordType : recordTypes) {
            if (features.contains(recordType.id)) {
                recordTypeParserBuilder.addRecordType(recordType);
            }
        }
        recordTypeParser = recordTypeParserBuilder.build();
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        initRecordTypeParser(schema.getFeatures());
        stationGeohashMap = StationLocationParser.getGeohashMapFromFile(
                new File("/Users/keegan/Sustain/etc/data/nwqmc_stations.csv"),
                schema.getGeohashLength());

    }

    @Override
    public void onDataAvailability(Map<String, Integer> columnMap, String[] splits) {
        recordTypeParser.getRecordTypeForLine(columnMap, splits);


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
