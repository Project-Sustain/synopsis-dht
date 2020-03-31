package sustain.synopsis.dht.journal;

import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.storage.lsmtree.ChecksumGenerator;

import java.io.*;
import java.util.Iterator;

/**
 * Logging functionality for journaling the storage activities. This implementation is not thread-safe - it assumes a
 * single writer model.
 */
public class Logger implements Iterable<byte[]> {
    private String filePath;
    private FileOutputStream fos;
    private DataOutputStream dos;
    private ChecksumGenerator checksumGenerator;
    private boolean initialized = false;
    private org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(Logger.class);

    public Logger(String filePath) {
        this.filePath = filePath;
    }

    public Logger(String filePath, ChecksumGenerator checksumGenerator) { // used for mocking in unit tests
        this.filePath = filePath;
        this.checksumGenerator = checksumGenerator;
    }

    private void initAppenders() throws StorageException {
        try {
            this.fos = new FileOutputStream(filePath, true); // open in the append mode
            this.dos = new DataOutputStream(this.fos);
            if (this.checksumGenerator == null) {
                this.checksumGenerator = new ChecksumGenerator();
            }
            this.initialized = true;
        } catch (IOException | ChecksumGenerator.ChecksumError e) {
            throw new StorageException("Error initializing journal log. Dir: " + filePath, e);
        }
    }

    public void append(byte[] payload) throws StorageException, IOException {
        if (!initialized) {
            initAppenders();
        }
        /* todo: we need to support atomic writes in this method. If there is an error in the middle, we
         should revert back the partial write. */
        byte[] checkSum = checksumGenerator.calculateChecksum(payload);
        dos.writeInt(payload.length);
        dos.write(payload);
        dos.writeInt(checkSum.length);
        dos.write(checkSum);
        dos.flush();
        fos.flush();
    }

    @Override
    public Iterator<byte[]> iterator() {
        JournalLogIterator iterator = new JournalLogIterator();
        File file = new File(filePath);
        if (file.exists()) {
            iterator.init();
        }
        return iterator;
    }

    public void close() throws StorageException {
        try {
            if (!initialized) {
                initAppenders(); // we need to write an empty file even if we close a log without appending any records
            }
            this.dos.close();
            this.fos.close();
        } catch (IOException e) {
            logger.error("Error closing streams for JournalLog", e);
        }
    }

    class JournalLogIterator implements Iterator<byte[]> {

        // we need a pushback stream to support hasNext().
        private PushbackInputStream pushbackInputStream;
        private DataInputStream dis;

        private boolean initialized = false;

        public JournalLogIterator() {

        }

        private void init() {
            try {
                FileInputStream fis = new FileInputStream(filePath);
                pushbackInputStream = new PushbackInputStream(fis);
                dis = new DataInputStream(pushbackInputStream);
                if (checksumGenerator == null) {
                    checksumGenerator = new ChecksumGenerator();
                }
                this.initialized = true;
            } catch (FileNotFoundException | ChecksumGenerator.ChecksumError e) {
                // We need to catch this exception because we cannot throw an exception or return null in the
                // iterator() method of the JournalLog.
                // Instead, we return an empty iterator
                logger.error("Error initializing the iterator", e);
            }
        }

        @Override
        public boolean hasNext() {
            if (!this.initialized) {
                // this is possible because we catch the exception in the constructor
                // we do not attempt to initialize here because users only can get the iterator using
                // iterator() where the init() is invoked.
                logger.warn("Attempting to use an uninitialized iterator.");
                return false;
            }
            try {
                int b = pushbackInputStream.read();
                pushbackInputStream.unread(b);
                return b != -1;
            } catch (IOException e) {
                logger.error("Error in the iterator", e);
                return false;
            }
        }

        @Override
        public byte[] next() {
            try {
                byte[] payload = new byte[dis.readInt()];
                dis.readFully(payload);
                byte[] checkSum = new byte[dis.readInt()];
                dis.readFully(checkSum);
                if (checksumGenerator.validateChecksum(payload, checkSum)) {
                    return payload;
                } else {
                    logger.warn("Corrupt log record is detected. Checksum validation failed.");
                }
            } catch (IOException e) {
                logger.error("Error in the iterator", e);
            }
            return null; // null means a corrupted record or IO error
        }
    }

}
