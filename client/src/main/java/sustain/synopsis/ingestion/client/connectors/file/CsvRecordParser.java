package sustain.synopsis.ingestion.client.connectors.file;

import com.opencsv.CSVParser;
import sustain.synopsis.ingestion.client.core.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CsvRecordParser implements RecordParser {

    private boolean skipHeader;
    private List<FeatureParser> featureParsers;

    private CsvRecordParser() {}

    @Override
    public Record parse(String line, int lineNum) {
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

    public static CsvRecordParserBuilder newBuilder() {
        return new CsvRecordParserBuilder();
    }

    public static class CsvRecordParserBuilder {
        private boolean skipHeader = false;
        private List<FeatureParser> featureParsers = new ArrayList<>();

        private CsvRecordParserBuilder() {}

        public CsvRecordParser build() {
            CsvRecordParser csvRecordParser = new CsvRecordParser();
            csvRecordParser.skipHeader = skipHeader;
            csvRecordParser.featureParsers = featureParsers;

            return csvRecordParser;
        }

        public CsvRecordParserBuilder setSkipHeader(boolean skipHeader) {
            this.skipHeader = skipHeader;
            return this;
        }

        public CsvRecordParserBuilder setTimeStampColumn(int columnIdx) {
            featureParsers.add((record, splits) -> {
                record.setTimestamp(Long.parseLong(splits[columnIdx]));
            });
            return this;
        }

        public CsvRecordParserBuilder withFeatureColumn(int columnIdx, String featureName) {
            featureParsers.add((record, splits) -> {
                record.addFeatureValue(featureName, Float.parseFloat(splits[columnIdx]));
            });
            return this;
        }

        public CsvRecordParserBuilder withFeatureParser(FeatureParser featureParser) {
            featureParsers.add(featureParser);
            return this;
        }

    }

    public interface FeatureParser {
        void parseFeatureIntoRecordFromSplits(Record record, String[] splits);
    }

}
