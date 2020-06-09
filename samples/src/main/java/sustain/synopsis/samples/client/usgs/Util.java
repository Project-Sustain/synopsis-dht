package sustain.synopsis.samples.client.usgs;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Util {

    static List<File> getFilesRecursive(File dir, int skipCount) {
        List<File> ret = new ArrayList<>();
        File[] files = dir.listFiles();
        int curSkipped = skipCount;
        for (int i = 0; i < files.length; i++) {
            File file = files[i];
            if (file.isDirectory()) {
                ret.addAll(getFilesRecursive(file, skipCount));
            } else if (curSkipped >= skipCount) {
                ret.add(file);
                curSkipped = 0;
            } else {
                curSkipped++;
            }
        }

        return ret;
    }

    static void getFilenamesRecursiveHelper(File dir, Set<String> set) {
        for (File f : dir.listFiles()) {
            if (f.isDirectory()) {
                getFilenamesRecursiveHelper(f, set);
            } else {
                set.add(f.getName());
            }
        }
    }

    public static Set<String> getFileNamesFromDirectoryRecursive(String dirPath) {
        Set<String> fileNames = new HashSet<>();
        File dir = new File(dirPath);
        if (dir.exists() && dir.isDirectory()) {
            getFilenamesRecursiveHelper(dir, fileNames);
        }
        return fileNames;
    }

    static void getStationIdHelper(File file, Set<String> set) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))))) {

            boolean inSiteData = false;
            while (br.ready()) {
                String line = br.readLine();


                if (inSiteData) {
                    String prefix2 = "# -";
                    if (line.length() >= prefix2.length() && line.startsWith(prefix2)) {
                        break;
                    }

                    String[] splits = line.split(" +", 4);
                    String org = splits[1];
                    String id = splits[2];

                    String key = org+"-"+id;
                    set.add(key);

                } else {
                    String prefix = "# Data for the following";
                    if (line.length() >= prefix.length() && line.startsWith(prefix)) {
                        inSiteData = true;
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    static List<String> getAllStationIds(List<File> files) {
        Set<String> idSet = new HashSet<>();
        int finished = 0;
        for (File f : files) {
            getStationIdHelper(f, idSet);
            if (++finished % 100 == 0) {
                System.out.println(finished+" "+f.getName()+" "+idSet.size());
            }
        }

        List<String> list = new ArrayList<>(idSet);
        list.sort(Comparator.naturalOrder());

        return list;
    }

    public static void main(String[] args) {
        File inputDir = new File(args[0]);
        File outFile = new File(args[1]);

        List<File> files = getFilesRecursive(inputDir, 0);
        System.out.println("File list size: "+files.size());

        List<String> allStationIds = getAllStationIds(files);
        System.out.println("Complete");

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(outFile))) {
            for (String id : allStationIds) {
                bw.write(id+"\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
