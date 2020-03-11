package sustain.synopsis.samples.client;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class DatasetAnalyzer {

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
    }

    public void printCounts(int limit) {
        ArrayList<RecordData> countsList = new ArrayList<>(types.values());
        countsList.sort((a,b) -> Integer.compare(b.count,a.count));
        for (int i = 0; i < countsList.size() && i < limit; i++) {
            RecordData recordData = countsList.get(i);
            System.out.print(getRecordDataStringRepr(recordData));
        }
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

    private String getRecordDataStringRepr(RecordData r) {
        return String.format("%d\n%s\n%s\n", r.count, getHeaderForBitSet(r.containedFields), getMinimalStringForLine(r.example));
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

        datasetAnalyzer.printCounts(10);
    }

}
