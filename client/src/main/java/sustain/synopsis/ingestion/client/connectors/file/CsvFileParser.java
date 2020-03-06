package sustain.synopsis.ingestion.client.connectors.file;

import com.opencsv.CSVParser;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvFileParser implements FileParser {
    private RecordCallbackHandler handler;
    private boolean skipHeader;
    private List<FeatureParser> featureParsers;

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
                handler.onRecordAvailability(record);
                lineCount++;
            }
        } catch (IOException e) { }
    }

    private Record parse(String line, int lineNum) {
        if (skipHeader && lineNum == 0) {
            return null;
        }

        try {
            String[] splits = new CSVParser().parseLine(line);
            Record record = new Record();
            for (FeatureParser featureParser: featureParsers) {
                featureParser.parseFeatureIntoRecordFromSplits(record, splits);
            }
            return record;

        } catch (IOException e) {
            return null;
        }
    }

    public static class CsvFileParserBuilder {
        private boolean skipHeader = false;
        private List<FeatureParser> featureParsers = new ArrayList<>();

        private CsvFileParserBuilder() {}

        public CsvFileParser build() {
            CsvFileParser csvRecordParser = new CsvFileParser();
            csvRecordParser.skipHeader = skipHeader;
            csvRecordParser.featureParsers = featureParsers;

            return csvRecordParser;
        }

        public CsvFileParserBuilder setSkipHeader(boolean skipHeader) {
            this.skipHeader = skipHeader;
            return this;
        }

        public CsvFileParserBuilder setTimeStampColumn(int columnIdx) {
            featureParsers.add((record, splits) -> {
                record.setTimestamp(Long.parseLong(splits[columnIdx]));
            });
            return this;
        }

        public CsvFileParserBuilder withFeatureColumn(int columnIdx, String featureName) {
            featureParsers.add((record, splits) -> {
                record.addFeatureValue(featureName, Float.parseFloat(splits[columnIdx]));
            });
            return this;
        }

        public CsvFileParserBuilder withFeatureParser(FeatureParser featureParser) {
            featureParsers.add(featureParser);
            return this;
        }

    }

    public interface FeatureParser {
        void parseFeatureIntoRecordFromSplits(Record record, String[] splits);
    }

}
