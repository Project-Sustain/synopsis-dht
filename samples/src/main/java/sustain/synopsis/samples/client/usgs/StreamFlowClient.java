package sustain.synopsis.samples.client.usgs;

import com.opencsv.exceptions.CsvValidationException;
import sustain.synopsis.ingestion.client.core.*;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StreamFlowClient {

    public static final String DISCHARGE_FEATURE = "discharge_cubic_feet_per_second";
    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
    public final static Pattern datePattern = Pattern.compile("\\d\\d\\d\\d_\\d\\d_\\d\\d");
    public static boolean isFileInDateRange(String fileName, Date beginDate, Date endDate) {
        Matcher matcher = datePattern.matcher(fileName);
        try {
            if (!matcher.find()) {
                return false;
            }
            Date date1 = dateFormat.parse(matcher.group());
            if (!matcher.find()) {
                return false;
            }
            Date date2 = dateFormat.parse(matcher.group());
            return date1.compareTo(beginDate) >= 0 && date2.compareTo(endDate) <= 0;

        } catch (ParseException e) {
            return false;
        }
    }

    static boolean dateRangeContainsYear(int year, int beginYear, int endYear) {
        boolean outOfRange = beginYear > year || endYear < year;
        return !outOfRange;
    }


    // TODO make cleaner solution to filtering directories based on year
    static ArrayList<File> getMatchingFiles(File[] files, Date beginDate, Date endDate, int beginYear, int endYear) {
        ArrayList<File> fileList = new ArrayList<>();
        for (File f : files) {
            if (f.isDirectory()) {
                int year = Integer.parseInt(f.getName());
                if (dateRangeContainsYear(year, beginYear, endYear)) {
                    fileList.addAll(getMatchingFiles(f.listFiles(), beginDate, endDate, beginYear, endYear));
                }
            } else {
                boolean fileMatches = f.getName().startsWith("stream_flow")
                        && f.getName().endsWith(".gz")
                        && isFileInDateRange(f.getName(), beginDate, endDate);
                if (fileMatches) {
                    fileList.add(f);
                }
            }
        }
        return fileList;
    }

    // example command line args:
    // localhost:9099
    // stream-flow-dataset
    // 1
    // /s/lattice-1/b/nobackup/galileo/stream-flow-data/stream_flow_bin_configuration.csv
    // /s/lattice-1/b/nobackup/galileo/stream-flow-data/stream_flow_co_stations.csv
    // /s/lattice-1/b/nobackup/galileo/stream-flow-data/co
    // 2016_01_01
    // 2020_01_01

    public static void main(String[] args) throws IOException, CsvValidationException, ParseException {

        if (args.length < 7) {
            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigFile stationLocationFile baseDir beginDate endDate");
            System.out.println("Dates in format yyyy_MM_dd");
            return;
        }

        String dhtNodeAddress = args[0];
        String datasetId = args[1];
        long sessionId = Long.parseLong(args[2]);
        String binConfigPath = args[3];
        File stationLocationFile = new File(args[4]);
        File baseDir = new File(args[5]);
        Date beginDate = dateFormat.parse(args[6]);
        Date endDate = dateFormat.parse(args[7]);
        int beginYear = Integer.parseInt(args[6].substring(0,4));
        int endYear =  Integer.parseInt(args[7].substring(0,4));

        List<File> inputFiles = getMatchingFiles(baseDir.listFiles(), beginDate, endDate, beginYear, endYear);
        inputFiles.sort(Comparator.comparing(File::getName));
        System.out.println("Total matching file count: " + inputFiles.size());

        Map<String, StationParser.Location> stationMap = StationParser.parseFile(stationLocationFile);
        System.out.println("Num stations in stationLocationFile: " + stationMap.size());
        SessionSchema sessionSchema = new SessionSchema(Util.quantizerMapFromFile(binConfigPath), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);

        StrandPublisher strandPublisher = new SimpleStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
        StrandRegistry strandRegistry = new StrandRegistry(strandPublisher, 10000, 100);

        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(TEMPORAL_BRACKET_LENGTH);
        RecordCallbackHandler recordCallbackHandler = new StreamFlowRecordCallbackHandler(strandRegistry, sessionSchema, temporalQuantizer);

        StreamFlowFileParser streamFlowFileParser = new StreamFlowFileParser(stationMap);
        streamFlowFileParser.initWithSchemaAndHandler(sessionSchema, recordCallbackHandler);

        for (File f : inputFiles) {
            streamFlowFileParser.parse(f);
        }

        recordCallbackHandler.onTermination();

    }

}
