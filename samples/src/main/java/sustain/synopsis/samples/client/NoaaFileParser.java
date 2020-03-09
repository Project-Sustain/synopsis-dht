package sustain.synopsis.samples.client;

import sustain.synopsis.ingestion.client.connectors.file.CsvFileParser;
import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.File;

public class NoaaFileParser implements FileParser {

    CsvFileParser csvFileParser;

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {

        CsvFileParser.CsvFileParserBuilder builder = CsvFileParser.newBuilder()
                .setSkipHeader(true);

        for (String feature : schema.getFeatures()) {
        }

    }

    @Override
    public void parse(File file) {

    }

}
