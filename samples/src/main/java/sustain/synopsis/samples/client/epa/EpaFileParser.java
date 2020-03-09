package sustain.synopsis.samples.client.epa;

import sustain.synopsis.ingestion.client.connectors.file.CsvFileParser;
import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.sketch.util.Geohash;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;

public class EpaFileParser implements FileParser {

    private CsvFileParser csvFileParser;

    public EpaFileParser() {
        csvFileParser = CsvFileParser.newBuilder()
                .setSkipHeader(true)
                .withFeatureParser((record, splits) -> {
                    record.setTimestamp(LocalDateTime.of(LocalDate.parse(splits[11].replace("\"","")),
                            LocalTime.parse(splits[12].replace("\"",""))).atZone(ZoneId.of(
                            "GMT")).toInstant().toEpochMilli());
                })
                .withFeatureParser((record, splits) -> {
                    record.setGeohash(Geohash.encode(Float.parseFloat(splits[5]), Float.parseFloat(splits[6]), 3));
                })
                .withFeatureParser(((record, splits) -> {
                    record.addFeatureValue(splits[8].replace("\"",""), Float.parseFloat(splits[13]));
                }))
                .build();
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        csvFileParser.initWithSchemaAndHandler(schema, handler);
    }

    @Override
    public void parse(File file) {
        csvFileParser.parse(file);
    }

}
