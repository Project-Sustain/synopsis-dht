package sustain.synopsis.dht.store;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class DiskManager {

    private static DiskManager instance;

    public static DiskManager getInstance() throws StorageException {
        if (instance == null) {
            synchronized (DiskManager.class) {
                if (instance == null) {
                    instance = new DiskManager();
                    boolean success = instance.init(Context.getInstance().getNodeConfig());
                    if (!success) {
                        throw new StorageException("Disk Manager initialization Error.");
                    }
                }
            }
        }
        return instance;
    }

    static class StorageDirectory {
        String path;
        long availableCapacity;

        public StorageDirectory(String path, long availableCapacity) {
            this.path = path;
            this.availableCapacity = availableCapacity;
        }
    }

    private final Logger logger = Logger.getLogger(DiskManager.class);
    private List<StorageDirectory> directories = new ArrayList<>();

    private DiskManager() {
    }

    boolean init(NodeConfiguration nodeConfiguration) {
        if (nodeConfiguration == null || nodeConfiguration.getStorageDirs() == null) {
            logger.error("Error initializing the DiskManager. A Node Configuration is not provided.");
            return false;
        }
        List<String> paths = nodeConfiguration.getStorageDirs();
        long totalAvailableSpace = 0;
        for (String path : paths) {
            StorageDirectory storageDirectory = processPath(new File(path));
            if (storageDirectory == null) {
                continue;
            }
            directories.add(storageDirectory);
            totalAvailableSpace += storageDirectory.availableCapacity;
            if (logger.isDebugEnabled()) {
                logger.debug("Added path: " + storageDirectory.path + ", capacity: " + storageDirectory.availableCapacity);
            }
        }
        if (directories.size() == 0) {
            logger.error("Error initializing disk manager. No writable directories.");
            return false;
        }
        logger.info("Usable directory count: " + directories.size() + ", total available space: " + totalAvailableSpace);
        return true;
    }

    StorageDirectory processPath(File f) {
        System.out.println("processing path " + f.getAbsolutePath());
        if (!f.exists()) { // path does not exist. Attempt top create the path
            boolean status = f.mkdirs();
            if (!status) {
                logger.error("Unable to create path: " + f.getAbsolutePath());
                return null;
            }
        } else {    // path exists, but it is a file.
            if (!f.isDirectory()) {
                logger.error("Provided path is not a directory. Path: " + f.getAbsolutePath());
                return null;
            }
        }
        long availableSpace = f.getFreeSpace();
        if (availableSpace == 0) { // no disk space
            logger.error("No available space in path: " + f.getAbsolutePath());
            return null;
        }
        return new StorageDirectory(f.getAbsolutePath(), availableSpace);
    }

    String allocate(int size) {
        return null;
    }
}
