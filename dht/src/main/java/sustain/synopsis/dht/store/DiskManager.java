package sustain.synopsis.dht.store;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.NodeConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class DiskManager {

    private final Logger logger = Logger.getLogger(DiskManager.class);
    private List<StorageDirectory> directories = Collections.synchronizedList(new ArrayList<>());
    private AllocationPolicy allocationPolicy;

    public boolean init(NodeConfiguration nodeConfiguration) {
        if (nodeConfiguration == null || nodeConfiguration.getStorageDirs() == null) {
            logger.error("Error initializing the DiskManager. A Node Configuration is not provided.");
            return false;
        }

        this.allocationPolicy =
                AllocationPolicyFactory.getAllocationPolicy(nodeConfiguration.getStorageAllocationPolicy());
        if (this.allocationPolicy == null) {
            logger.error(
                    "Unable to find a matching storage allocation policy for the provided option: " + nodeConfiguration
                            .getStorageAllocationPolicy());
            return false;
        }

        Set<String> paths = nodeConfiguration.getStorageDirs().keySet();
        long totalAvailableSpace = 0;
        for (String path : paths) {
            StorageDirectory storageDirectory =
                    processPath(new File(path), nodeConfiguration.getStorageDirs().get(path) * 1024 * 1024 * 1024);
            if (storageDirectory == null) {
                continue;
            }
            directories.add(storageDirectory);
            totalAvailableSpace += storageDirectory.allocatedCapacity;
            if (logger.isDebugEnabled()) {
                logger.debug(
                        "Added path: " + storageDirectory.path + ", capacity: " + storageDirectory.allocatedCapacity);
            }
        }
        if (directories.size() == 0) {
            logger.error("Error initializing disk manager. No writable directories.");
            return false;
        }
        logger.info(
                "Usable directory count: " + directories.size() + ", total available space: " + totalAvailableSpace);
        return true;
    }

    StorageDirectory processPath(File f, long allocatedSpace) {
        logger.info("processing path " + f.getAbsolutePath());
        long occupiedSpace = 0;
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
            // check if the allocated disk space is available
            occupiedSpace = getDirectorySize(f);
            if (occupiedSpace >= allocatedSpace) {
                logger.warn(
                        "Allocated space < occupied space for path: " + f.getAbsolutePath() + ". Allocated " + "space: "
                        + allocatedSpace + ", Occupied space: " + occupiedSpace);
            }
        }
        long availableSpace = f.getFreeSpace();
        if (availableSpace == 0) { // no disk space
            logger.error("No available space in path: " + f.getAbsolutePath());
            return null;
        }
        return new StorageDirectory(f, allocatedSpace, occupiedSpace, availableSpace);
    }

    public long getDirectorySize(File directory) {
        return FileUtils.sizeOfDirectory(directory);
    }

    public String allocate(long size) throws StorageException {
        StorageDirectory directory = allocationPolicy.select(size, directories);
        if (directory == null) {
            throw new StorageException("Could not find a storage location.");
        }
        return directory.path.getAbsolutePath();
    }

    static class StorageDirectory {
        File path;
        long allocatedCapacity;
        long occupiedCapacity;
        long availableSpace;

        StorageDirectory(File path, long allocatedCapacity, long occupiedCapacity, long availableSpace) {
            this.path = path;
            this.allocatedCapacity = allocatedCapacity;
            this.occupiedCapacity = occupiedCapacity;
            this.availableSpace = availableSpace;
        }

        synchronized boolean allocate(long requestedCapacity) {
            if (occupiedCapacity >= allocatedCapacity) {
                return false;
            }
            // check if it exceeds allocated space significantly
            if ((occupiedCapacity + requestedCapacity) > (allocatedCapacity * 1.10)) { // allow a 10% buffer. to
                // reduce fragmentation.
                return false;
            }
            // there can be disk writes from other entities/processes
            availableSpace = path.getUsableSpace();
            if (availableSpace < requestedCapacity) {
                return false;
            }
            occupiedCapacity += requestedCapacity;
            availableSpace -= requestedCapacity;
            // check if there is space in the disk
            return true;
        }
    }
}
