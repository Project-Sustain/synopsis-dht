package sustain.synopsis.samples.client;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.core.Record;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DatasetAnalyzer {

    int totalRows = 0;
    String[] header = null;
    Map<BitSet, RecordData> types = new HashMap<>();

    public DatasetAnalyzer() { }

    public void setHeader(String[] header) {
        this.header = header;
    }

    public void acceptRecord(String[] line) {
        BitSet lineBitSet = getBitSetForLine(line);
        RecordData newRecordData = new RecordData(lineBitSet, line, 1);
        if (types.putIfAbsent(lineBitSet, newRecordData) != null) {
            RecordData existingData = types.get(lineBitSet);
            existingData.count++;
        }
        totalRows++;
    }

    public List<RecordData> getTopCounts(int n) {
        ArrayList<RecordData> countsList = new ArrayList<>(types.values());
        countsList.sort((a,b) -> Integer.compare(b.count,a.count));
        return countsList.subList(0,Math.min(n,countsList.size()));
    }

    public void printRecordDataList(List<RecordData> recordDataList) {
        for (RecordData r : recordDataList) {
            System.out.printf("%d\n%s\n%s\n", r.count, getHeaderForBitSet(r.containedFields), getMinimalStringForLine(r.example));
        }
    }

    public void printUniqueHeaderFields(List<RecordData> recordDataList) {
        BitSet commonFields = getCommonFieldsInList(recordDataList);
        for (RecordData r : recordDataList) {
            BitSet uniqueFields = (BitSet) r.containedFields.clone();
            uniqueFields.andNot(commonFields);
            System.out.printf("%d\n%s\n%s\n", r.count, getHeaderForBitSet(uniqueFields), getMinimalStringForLine(r.example));
        }
    }

    public BitSet getCommonFieldsInList(List<RecordData> recordDataList) {
        if (recordDataList.size() == 0) {
            return null;
        }

        BitSet commonFeatures = recordDataList.get(0).containedFields;
        for (int i = 1; i < recordDataList.size(); i++) {
            commonFeatures.and(recordDataList.get(i).containedFields);
        }
        return commonFeatures;
    }

    private static String getMinimalStringForLine(String[] line) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < line.length-1; i++) {
            String s = line[i];
            if (!s.equals("")) {
                sb.append(s+",");
            }
        }
        if (line.length - 1 >= 0) {
            String s = line[line.length - 1];
            if (!s.equals("")) {
                sb.append(s);
            }
        }
        return sb.toString();
    }

    public String getStringForLineFields(String[] line, BitSet fields) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < line.length-1; i++) {
            if (fields.get(i)) {
                sb.append(line[i]+",");
            }
        }
        if (line.length - 1 >= 0) {
            if (fields.get(line.length-1)) {
                sb.append(line[line.length-1]);
            }
        }
        return sb.toString();
    }

    private static BitSet getBitSetForLine(String[] line) {
        BitSet bitSet = new BitSet(line.length);
        for (int i = 0; i < line.length; i++) {
            if (!line[i].equals("")) {
                bitSet.set(i);
            }
        }
        return bitSet;
    }

    private String getHeaderForBitSet(BitSet bitSet) {
        StringBuilder sb = new StringBuilder();
        bitSet.stream().forEach(i -> sb.append(header[i]+","));
        return sb.toString();
    }

    private static class RecordData {
        BitSet containedFields;
        String[] example;
        int count;

        public RecordData(BitSet containedFields, String[] example, int count) {
            this.containedFields = containedFields;
            this.example = example;
            this.count = count;
        }
    }

    public static void main(String[] args) throws IOException, CsvValidationException {
        String csvFilePath = args[0];
        CSVReader csvReader = new CSVReader(new BufferedReader(new FileReader(csvFilePath)));

        DatasetAnalyzer datasetAnalyzer = new DatasetAnalyzer();
        datasetAnalyzer.setHeader(csvReader.readNext());

        String[] line;
        while ((line = csvReader.readNext()) != null)
        {
            datasetAnalyzer.acceptRecord(line);
        }

        System.out.println("total records: "+datasetAnalyzer.totalRows);
        datasetAnalyzer.printUniqueHeaderFields(datasetAnalyzer.getTopCounts(5));
    }

}
