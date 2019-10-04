package synopsis2.dht.store.halodb;

import com.oath.halodb.HaloDB;
import com.oath.halodb.HaloDBException;
import com.oath.halodb.HaloDBOptions;
import org.apache.log4j.Logger;
import synopsis2.dht.store.Iterator;
import synopsis2.dht.store.Store;

public class HaloDBStore implements Store {

    private Logger logger = Logger.getLogger(HaloDBStore.class);

    private final HaloDBOptions options = new HaloDBOptions();
    private HaloDB db;
    private String path;

    public HaloDBStore(String path) {
        options.setMaxFileSize(50 * 1024 * 1024); // 50 MB files
        options.setFlushDataSizeBytes(10 * 1024 * 1024);
        options.setCompactionThresholdPerFile(0.7);
        options.setCompactionJobRate(50 * 1024 * 1024);
        options.setNumberOfRecords(100_000_000);
        options.setCleanUpTombstonesDuringOpen(true);
        options.setCleanUpInMemoryIndexOnClose(true);
        options.setUseMemoryPool(false); // todo: check if memory pool is required

        this.path = path;
    }


    @Override
    public boolean open() {
        try {
            db = HaloDB.open(path, options);
            logger.info("HaloDB instance is created successfully for path: " + path);
            return true;
        } catch (HaloDBException e) {
            logger.error("Error opening the store.", e);
        }
        return false;
    }

    @Override
    public boolean close() {
        return false;
    }

    @Override
    public boolean store(byte[] key, byte[] val) {
        try {
            db.put(key, val);
            return true;
        } catch (HaloDBException e) {
            logger.error("Error storing in HaloDB instance.", e);
        }
        return false;
    }

    @Override
    public Iterator getIterator() {
        try {
            return new HaloDBStoreIterator(db.newIterator());
        } catch (HaloDBException e) {
            logger.error("Error creating the iterator");
        }
        return null;
    }
}
