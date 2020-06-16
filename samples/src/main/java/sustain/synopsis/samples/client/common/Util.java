package sustain.synopsis.samples.client.common;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class Util {

    public static List<File> getFilesRecursive(File dir, int skipCount) {
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

    public static void getFilenamesRecursiveHelper(File dir, Set<String> set) {
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


    public static void copySampleToNewDir(String[] args) {
        File inputDir = new File(args[0]);
        File outputDir = new File(args[1]);

        List<File> filesRecursive = getFilesRecursive(inputDir, 30);
        for (File f : filesRecursive) {
            try {
                InputStream is = new FileInputStream(f);
                byte[] buf = new byte[is.available()];
                is.read(buf);
                is.close();

                File out = new File(outputDir.getPath()+"/"+f.getName());
                OutputStream os = new FileOutputStream(out);
                os.write(buf);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }



}
