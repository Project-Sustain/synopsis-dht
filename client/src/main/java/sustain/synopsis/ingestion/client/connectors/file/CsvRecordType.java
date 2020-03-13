package sustain.synopsis.ingestion.client.connectors.file;

import java.util.BitSet;
import java.util.HashMap;

public class CsvRecordType {

    private final String id;
    private final BitSet requiredColumns;
    private final HashMap<Integer, String> columnValueMustMatch;

    public CsvRecordType(String id, BitSet requiredColumns, HashMap<Integer, String> columnValueMustMatch) {
        this.id = id;
        this.requiredColumns = requiredColumns;
        this.columnValueMustMatch = columnValueMustMatch;
    }

    public CsvRecordTypeBuilder newBuilder() {
        return new CsvRecordTypeBuilder();
    }

    public static class CsvRecordTypeBuilder {

        private String id = "";
        private BitSet requiredColumns = new BitSet();
        private HashMap<Integer, String> columnValueMustMatch = new HashMap<>();

        private CsvRecordTypeBuilder() { }

        public CsvRecordType build() {
            return new CsvRecordType(id, requiredColumns, columnValueMustMatch);
        }

        public CsvRecordTypeBuilder setId(String id) {
            this.id = id;
            return this;
        }

        public CsvRecordTypeBuilder requiredColumn(int idx) {
            this.requiredColumns.set(idx);
            return this;
        }

        public CsvRecordTypeBuilder columnValueMustMatch(int idx, String regex) {
            this.requiredColumns.set(idx);
            this.columnValueMustMatch.put(idx, regex);
            return this;
        }

    }

}
