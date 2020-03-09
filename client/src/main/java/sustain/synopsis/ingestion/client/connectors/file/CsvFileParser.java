package sustain.synopsis.ingestion.client.connectors.file;

import com.opencsv.CSVParser;
import sustain.synopsis.ingestion.client.connectors.RecordParseException;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CsvFileParser implements FileParser {
    private RecordCallbackHandler handler;
    private boolean skipHeader = false;
    private boolean readHeader = false;
    private List<FeatureParser> featureParsers = new ArrayList<>();
    private Map<String, Integer> columnNameMap = new HashMap<>();

    private int skippedRecords = 0;
    private int processedRecords = 0;

    private CsvFileParser() {}

    public static CsvFileParserBuilder newBuilder() {
        return new CsvFileParserBuilder();
    }

    @Override
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler) {
        this.handler = handler;
    }

    @Override
    public void parse(File file) {
        int lineCount = 0;
        String line;

        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            while ((line = br.readLine()) != null) {
                Record record = parse(line, lineCount);
                if (record != null) {
                    handler.onRecordAvailability(record);
                }
                lineCount++;
            }
        } catch (IOException e) { }
    }

    public int getColumnIdx(String columnName) {
        return columnNameMap.get(columnName);
    }

    private Record parse(String line, int lineNum) {
        try {
            String[] splits = new CSVParser().parseLine(line);

            if (lineNum == 0) {
                if (skipHeader) {
                    return null;

                } else if (readHeader){
                    initColumnNameMap(splits);
                    return null;
                }
            }

            Record record = new Record();
            for (FeatureParser featureParser: featureParsers) {
                featureParser.parseFeatureIntoRecordFromSplits(record, splits);
            }
            processedRecords++;
            return record;

        } catch (Exception e) {
            skippedRecords++;
            return null;
        }
    }

    private void initColumnNameMap(String[] splits) {
        for (int i = 0; i < splits.length; i++) {
            columnNameMap.put(splits[i], i);
        }
    }

    public static class CsvFileParserBuilder {

        CsvFileParser csvFileParser = new CsvFileParser();

        private CsvFileParserBuilder() {}

        public CsvFileParser build() {
            return csvFileParser;
        }

        public CsvFileParserBuilder setSkipHeader(boolean skipHeader) {
            csvFileParser.skipHeader = skipHeader;
            return this;
        }

        public CsvFileParserBuilder setReadHeader(boolean readHeader) {
            csvFileParser.readHeader = readHeader;
            return this;
        }

        public CsvFileParserBuilder setTimeStampColumn(int columnIdx) {
            csvFileParser.featureParsers.add((record, splits) -> {
                record.setTimestamp(Long.parseLong(splits[columnIdx]));
            });
            return this;
        }

        public CsvFileParserBuilder withFeatureColumn(int columnIdx, String featureName) {
            csvFileParser.featureParsers.add((record, splits) -> {
                record.addFeatureValue(featureName, Float.parseFloat(splits[columnIdx]));
            });
            return this;
        }

        public CsvFileParserBuilder withFeatureColumn(String columnName, String featureName) {
            csvFileParser.featureParsers.add((record, splits) -> {
                record.addFeatureValue(featureName, Float.parseFloat(splits[csvFileParser.getColumnIdx(columnName)]));
            });
            return this;
        }

        public CsvFileParserBuilder withFeatureParser(FeatureParser featureParser) {
            csvFileParser.featureParsers.add(featureParser);
            return this;
        }

    }

    public interface FeatureParser {
        void parseFeatureIntoRecordFromSplits(Record record, String[] splits) throws RecordParseException;
    }

}
