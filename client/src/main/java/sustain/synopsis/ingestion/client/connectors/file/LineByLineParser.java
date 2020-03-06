package sustain.synopsis.ingestion.client.connectors.file;

import sustain.synopsis.ingestion.client.connectors.DataParser;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.BufferedReader;
import java.io.IOException;

public class LineByLineParser implements DataParser {

    final RecordParser recordParser;

    public LineByLineParser(RecordParser recordParser) {
        this.recordParser = recordParser;
    }

    @Override
    public void parseFromReaderWithHandler(BufferedReader br, RecordCallbackHandler handler) throws IOException {
        int lineCount = 0;
        String line;
        while ((line = br.readLine()) != null) {
            Record record = recordParser.parse(line, lineCount);
            handler.onRecordAvailability(record);
            lineCount++;
        }
    }


}
