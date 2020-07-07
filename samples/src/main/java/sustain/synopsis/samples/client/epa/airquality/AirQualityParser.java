package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.ingestion.client.core.BinCalculator;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.samples.client.common.Location;
import sustain.synopsis.samples.client.common.Util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class AirQualityParser {

    static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("\"yyyy-MM-dd\"\"kk:mm\"");

    static void parseFile(File f, HourlyDataEntryHandler handler) {
        try {
            ZipFile zf = new ZipFile(f);
            String zipEntryName = f.getName().substring(0, f.getName().length() - 3) + "csv";
            ZipEntry entry = zf.getEntry(zipEntryName);
            try (BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(entry)))) {
                br.readLine();
                while(br.ready()) {
                    String line = br.readLine();
                    String[] splits = line.split(",");

                    String siteId = (splits[0]+splits[1]+splits[2])
                            .replace("\"", "");
                    Location location = new Location(Float.parseFloat(splits[5]), Float.parseFloat(splits[6]));
                    LocalDateTime dateTime = LocalDateTime.parse(splits[11]+splits[12], dateTimeFormatter);
                    String parameterName = splits[8];
                    float measurement = Float.parseFloat(splits[13]);
                    String methodCode = splits[20];

                    HourlyDataEntry hde = new HourlyDataEntry(siteId, location, dateTime, parameterName, methodCode, measurement);
                    handler.onHourlyDataEntry(hde);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        File inputDir = new File(args[0]);
        int beginYear = Integer.parseInt(args[1]);
        int endYear = Integer.parseInt(args[2]);

        MyHourlyDataEntryHandler myHourlyDataEntryHandler = new MyHourlyDataEntryHandler();

        List<File> filesRecursive = Util.getFilesRecursive(inputDir, 0).stream()
                .filter(f -> f.getName().matches("hourly_.+_\\d\\d\\d\\d.zip"))
                .filter(f -> {
                    int year = Integer.parseInt(f.getName().substring(f.getName().length()-8, f.getName().length()-4));
                    return year >= beginYear && year <= endYear;
                }).collect(Collectors.toList());
        System.out.println(filesRecursive.size());

        for (File f : filesRecursive) {
            System.out.println(f.getName());
            parseFile(f, myHourlyDataEntryHandler);
        }
        System.out.println(myHourlyDataEntryHandler.count);

        BinCalculator.BinCalculatorResult binCalculatorResult = calculateBinConfiguration(myHourlyDataEntryHandler, 10000);
        System.out.println(binCalculatorResult.getMaxRmse());
        System.out.println(binCalculatorResult.toString());
    }

    static BinCalculator.BinCalculatorResult calculateBinConfiguration(MyHourlyDataEntryHandler entryHandler, int recordCountTarget) {
        Random random = new Random();

        List<Record> records = entryHandler.recordMap.values().stream()
                .filter(entryHandler::recordIsCompleted).collect(Collectors.toList());

        System.out.println(records.size());

        double target = (double) recordCountTarget / records.size();

        List<Record> recordsOut = new ArrayList<>();
        for (Record r : records) {
            if (random.nextDouble() < target) {
                recordsOut.add(r);
            }
        }

        BinCalculator binCalculator = BinCalculator.newBuilder().setMaxTicks(30)
                .setDiscErrorThreshold(0.025).build();

        BinCalculator.BinCalculatorResult binCalculatorResult = binCalculator.calculateBins(recordsOut);
        return binCalculatorResult;
    }

}
