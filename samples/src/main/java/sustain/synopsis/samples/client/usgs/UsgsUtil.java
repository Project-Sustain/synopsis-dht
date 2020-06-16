package sustain.synopsis.samples.client.usgs;

import sustain.synopsis.samples.client.common.Util;

import java.io.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.zip.GZIPInputStream;

public class UsgsUtil {

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

    static void writeStationIds(String[] args) {
        File inputDir = new File(args[0]);
        File outFile = new File(args[1]);

        List<File> files = Util.getFilesRecursive(inputDir, 0);
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

    static class FilePredicate implements Predicate<File> {

        final String state;
        final int yearStart;
        final int yearEnd;

        public FilePredicate(String state, int yearStart, int yearEnd) {
            this.state = state;
            this.yearStart = yearStart;
            this.yearEnd = yearEnd;
        }

        @Override
        public boolean test(File file) {
            int year = Integer.parseInt(file.getName().substring(3, 7));
            boolean yearMatches = year >= yearStart && year <= yearEnd;
            boolean stateMatches = file.getName().startsWith(state);

            return stateMatches && yearMatches;
        }
    }

    public static void main(String[] args) {
    }
}
