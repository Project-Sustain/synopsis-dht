package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.samples.client.common.Util;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class AirQualityClient {

    static final String[] possible = new String[]{"42401", "42101", "42602", "88101", "88502", "81102", "SPEC", "PM10SPEC"};

    static List<File> getFilteredList(List<File> files, String[] parameters, int yearStart, int yearEnd) {
        List<File> ret = new ArrayList<>();
        for (File f : files) {
            if (f.getName().length() < 12 || !f.getName().startsWith("hourly")) {
                continue;
            }

            int year = Integer.parseInt(f.getName().substring(f.getName().length()-8, f.getName().length()-4));
            boolean yearInRange = year >= yearStart && year <= yearEnd;
            if (!yearInRange) {
                continue;
            }

            boolean contains = false;
            for (String p : parameters) {
                if (f.getName().contains(p)) {
                    contains = true;
                    break;
                }
            }
            if (!contains) {
                continue;
            }

            ret.add(f);
        }
        return ret;
    }

    public static void main(String[] args) {
        File inputDir = new File(args[0]);
        int yearStart = Integer.parseInt(args[1]);
        int yearEnd = Integer.parseInt(args[2]);
        String[] parameters = args[3].split(",");

        List<File> filesRecursive = Util.getFilesRecursive(inputDir, 0);
        List<File> filteredFiles = getFilteredList(filesRecursive, parameters, yearStart, yearEnd);
        filteredFiles.sort(Comparator.naturalOrder());
//        filteredFiles.forEach(f -> System.out.println(f.getName()));

    }




}





















