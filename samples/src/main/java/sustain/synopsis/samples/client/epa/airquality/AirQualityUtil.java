package sustain.synopsis.samples.client.epa.airquality;

import sustain.synopsis.samples.client.common.Timer;

import java.io.*;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

public class AirQualityUtil {

//    static void convertFileToJson(File inFile, File outFile) {
//        try {
//            ZipFile zf = new ZipFile(f);
//            String zipEntryName = f.getName().substring(0, f.getName().length() - 3) + "csv";
//            ZipEntry entry = zf.getEntry(zipEntryName);
//
//            try (BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(entry)))) {
////                br.lines().findFirst().ifPresent(System.out::println);
//                br.readLine();
//                String lastId = "";
//                while(br.ready()) {
//                    String line = br.readLine();
//                    String[] splits = line.split(",", 4);
//                    String id = (splits[0]+splits[1]+splits[2])
//                            .replace("\"", "");
//
//                    if (id.equals())
//
//                        if (!id.equals(lastId)) {
//                            sites.add(id);
//                            lastId = id;
//                        } else {
//
//                        }
//                    sites.add(id);
//                }
//
//            }
//
//
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    static String getStringFromSplits(String[] splits, Set<Integer> ignoreColumns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < splits.length; i++) {
            if (!ignoreColumns.contains(i)) {
                sb.append(splits[i].replaceAll("\"", ""));
            }
        }
        return sb.toString();
    }

    static void addDifferences(String[] s1, String[] s2) {
        if (s1.length != s2.length) {

            System.exit(1231231);
        }
        for (int i = 0; i < s1.length; i++) {
            if (!s1[i].equals(s2[i])) {
                differences[i]++;
            }
        }
    }

    static void addSitesFromFile(File f, Map<String, String> siteData) {
//        try (BufferedReader br = new BufferedReader(new InputStreamReader(new ZipInputStream(new FileInputStream(f))))) {


        Map<String, String[]> map = new HashMap<>();

        Set<Integer> ignoreColumns = new HashSet<>();
        ignoreColumns.add(9);
        ignoreColumns.add(10);
        ignoreColumns.add(11);
        ignoreColumns.add(12);
        ignoreColumns.add(13);


        try {
            ZipFile zf = new ZipFile(f);
            String zipEntryName = f.getName().substring(0, f.getName().length() - 3) + "csv";
            ZipEntry entry = zf.getEntry(zipEntryName);

            try (BufferedReader br = new BufferedReader(new InputStreamReader(zf.getInputStream(entry)))) {
//                br.lines().findFirst().ifPresent(System.out::println);
                br.readLine();
                String lastId = "";
                while(br.ready()) {
                    String line = br.readLine();
                    String[] splits = line.split(",");
                    String id = (splits[0]+splits[1]+splits[2])
                            .replace("\"", "");

                    if (id.equals(lastId)) {
                        addDifferences(map.get(id), splits);

                    } else {
                        map.put(id, splits);
                        lastId = id;
                    }


                }

            }



        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void writeSitesToFile(File f, Set<String> sites) {

    }

    static int[] differences = new int[24];

    public static void main(String[] args) {
//        File inputDir = new File(args[0]);
//        File outputFile = new File(args[1]);
//        List<File> files = Util.getFilesRecursive(inputDir, 0);

//        File inputFile = new File(args[0]);
//
//        Map<String, String> ids = new HashMap<>();
//
//        Timer timer = new Timer();
//
//        timer.start();
//        addSitesFromFile(inputFile, ids);
//        timer.stop();
//
//        System.out.println(ids.size());
//        System.out.println(timer.millis());
//
//        String[] names = new String[]{"State Code","County Code","Site Num","Parameter Code","POC","Latitude","Longitude","Datum","Parameter Name","Date Local","Time Local","Date GMT","Time GMT","Sample Measurement","Units of Measure","MDL","Uncertainty","Qualifier","Method Type","Method Code","Method Name","State Name","County Name","Date of Last Change"};
//
//        for (int i = 0; i < differences.length; i++) {
//            System.out.println(names[i]+": "+differences[i]);
//        }

    }


}
