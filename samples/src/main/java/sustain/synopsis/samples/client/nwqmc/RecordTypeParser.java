package sustain.synopsis.samples.client.nwqmc;

import java.util.List;
import java.util.Map;

public class RecordTypeParser {

    private final Map<String, CsvRecordType> recordTypes;

    private RecordTypeParser(Map<String, CsvRecordType> recordTypes) {
        this.recordTypes = recordTypes;
    }

    private boolean lineDoesMatchRecordType(CsvRecordType recordType, Map<String,Integer> headerMap, String[] splits) {
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

    public CsvRecordType getRecordTypeForLine(Map<String,Integer> headerMap, String[] splits) {
        for (String recordTypeId : recordTypes.keySet()) {
            CsvRecordType recordType = recordTypes.get(recordTypeId);
            if (lineDoesMatchRecordType(recordType, headerMap, splits)) {
                return recordType;
            }
        }
        return null;
    }

    public static RecordTypeParserBuilder newBuilder() {
        return new RecordTypeParserBuilder();
    }

    public static class RecordTypeParserBuilder {

        private Map<String, CsvRecordType> recordTypes;

        private RecordTypeParserBuilder() {}

        public RecordTypeParser build() {
            return new RecordTypeParser(recordTypes);
        }

        public RecordTypeParserBuilder addRecordType(String id, CsvRecordType recordType) {
            this.recordTypes.put(id, recordType);
            return this;
        }

    }

}
