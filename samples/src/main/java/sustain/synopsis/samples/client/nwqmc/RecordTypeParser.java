package sustain.synopsis.samples.client.nwqmc;

import java.util.List;
import java.util.Map;

public class RecordTypeParser {

    private final List<RecordType> recordTypes;

    private RecordTypeParser(List<RecordType> recordTypes) {
        this.recordTypes = recordTypes;
    }

    private boolean lineDoesMatchRecordType(RecordType recordType, Map<String,Integer> headerMap, String[] splits) {
        for (String colName : recordType.columnMatches.keySet())
        {
            Integer colIdx = headerMap.get(colName);
            if (colIdx == null) {
                return false;
            }

            String colValue = splits[colIdx];
            if (!colValue.matches(recordType.columnMatches.get(colName))) {
                return false;
            }
        }
        return true;
    }

    public RecordType getRecordTypeForLine(Map<String,Integer> columnMap, String[] splits) {
        for (RecordType recordType : recordTypes) {
            if (lineDoesMatchRecordType(recordType, columnMap, splits)) {
                return recordType;
            }
        }
        return null;
    }

    public static RecordTypeParserBuilder newBuilder() {
        return new RecordTypeParserBuilder();
    }

    public static class RecordTypeParserBuilder {

        private List<RecordType> recordTypes;

        private RecordTypeParserBuilder() {}

        public RecordTypeParser build() {
            return new RecordTypeParser(recordTypes);
        }

        public RecordTypeParserBuilder addRecordType(RecordType recordType) {
            this.recordTypes.add(recordType);
            return this;
        }

    }

}
