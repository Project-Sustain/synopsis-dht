package sustain.synopsis.ingestion.client.connectors.file;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class CsvFileParser implements FileParser {

    private LineHandler lineHandler;
    private Map<String, Integer> columnMap = new HashMap<>();

    public CsvFileParser(LineHandler lineHandler) {
        this.lineHandler = lineHandler;
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        this.lineHandler.initWithSchemaAndHandler(schema, handler);
    }

    @Override
    public void parse(File file) {
        int lineCount = 0;
        String[] splits;

        try (CSVReader reader = new CSVReader(new FileReader(file))) {
            while ((splits = reader.readNext()) != null) {
                if (lineCount == 0) {
                    initColumnMap(splits);
                }
                this.lineHandler.onDataAvailability(columnMap, splits);

                lineCount++;
            }
        } catch (IOException | CsvValidationException e) { }
    }

    private void initColumnMap(String[] splits) {
        for (int i = 0; i < splits.length; i++) {
            columnMap.put(splits[i], i);
        }
    }

}
