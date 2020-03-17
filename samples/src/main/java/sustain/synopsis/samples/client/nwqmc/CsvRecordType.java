package sustain.synopsis.samples.client.nwqmc;

import java.util.HashMap;
import java.util.Map;

public class CsvRecordType {

    final Map<String, String> columnMatches;

    private CsvRecordType(Map<String, String> requiredColumns) {
        this.columnMatches = requiredColumns;
    }

    public static CsvRecordTypeBuilder newBuilder() {
        return new CsvRecordTypeBuilder();
    }

    public static class CsvRecordTypeBuilder {
        private Map<String, String> columnMatches = new HashMap<>();

        private CsvRecordTypeBuilder() { }

        public CsvRecordType build() {
            return new CsvRecordType(columnMatches);
        }

        public CsvRecordTypeBuilder columnMustMatch(String column, String regex) {
            this.columnMatches.put(column, regex);
            return this;
        }

    }

}
