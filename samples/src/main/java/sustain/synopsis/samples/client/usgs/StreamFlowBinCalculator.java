package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.core.BinCalculator;
import sustain.synopsis.ingestion.client.core.ListRecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.SessionSchema;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.time.Duration;
import java.util.*;

public class StreamFlowBinCalculator {

    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BUCKET_LENGTH = Duration.ofHours(6);

    public static void main(String[] args) throws ParseException, IOException, CsvValidationException {

        File baseDir = new File(args[0]);
        File stationsFile = new File(args[1]);
        Date beginDate = StreamFlowClient.dateFormat.parse(args[2]);
        Date endDate = StreamFlowClient.dateFormat.parse(args[3]);
        int beginYear = Integer.parseInt(args[2].substring(0,4));
        int endYear =  Integer.parseInt(args[3].substring(0,4));
        // fraction of files to be read is 1/fractionDenominator
        int fractionDenominator = Integer.parseInt(args[4]);

        List<File> inputFiles = StreamFlowClient.getMatchingFiles(baseDir.listFiles(), beginDate, endDate, beginYear, endYear);
        inputFiles.sort(Comparator.comparing(File::getName));
        System.out.println("Total matching file count: " + inputFiles.size());

        ListRecordCallbackHandler handler = new ListRecordCallbackHandler();
        StreamFlowFileParser fileParser = new StreamFlowFileParser(StationParser.parseFile(stationsFile));
        fileParser.initWithSchemaAndHandler(new SessionSchema(null, GEOHASH_LENGTH, TEMPORAL_BUCKET_LENGTH), handler);

        for (int i  = 0; i < inputFiles.size(); i += fractionDenominator) {
            fileParser.parse(inputFiles.get(i));
        }

        ArrayList<Record> records = handler.getRecords();
        System.out.println(records.size());

        String binConfiguration = new BinCalculator().getBinConfiguration(handler.getRecords());
        System.out.println(binConfiguration);

    }

}
