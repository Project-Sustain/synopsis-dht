package sustain.synopsis.samples.client.nwqmc;

import java.util.HashMap;
import java.util.Map;

public class RecordType {

    final String id;
    final Map<String, String> columnMatches;

    private RecordType(String id, Map<String, String> requiredColumns) {
        this.id = id;
        this.columnMatches = requiredColumns;
    }

    public static RecordTypeBuilder newBuilder() {
        return new RecordTypeBuilder();
    }

    public static class RecordTypeBuilder {
        private String id;
        private Map<String, String> columnMatches = new HashMap<>();

        private RecordTypeBuilder() { }

        public RecordType build() {
            return new RecordType(id, columnMatches);
        }

        public RecordTypeBuilder setId(String id) {
            this.id = id;
            return this;
        }

        public RecordTypeBuilder columnMustMatch(String column, String regex) {
            this.columnMatches.put(column, regex);
            return this;
        }

    }

}
