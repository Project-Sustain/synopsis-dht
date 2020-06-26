package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.samples.client.common.Location;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
        File inputFile = new File(args[0]);

        MyHourlyDataEntryHandler myHourlyDataEntryHandler = new MyHourlyDataEntryHandler();
        parseFile(inputFile, myHourlyDataEntryHandler);
        System.out.println(myHourlyDataEntryHandler.count);
    }

}
