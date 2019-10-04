package sustain.synopsis.ingestion.client.connectors.file;

import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.core.DataConnector;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A data connector to read records from a list of files. '\n' character is used to separate the records.
 * Each line is converted to a record using the provided {@link FileParserHelper}.
 * A single threaded executor is used to run the data connector.
 */
public class FileDataConnector implements DataConnector, Runnable {
    private final Logger logger = Logger.getLogger(FileDataConnector.class);
    private final FileParserHelper parserHelper;
    private final RecordCallbackHandler recordCallbackHandler;
    private final File[] input;
    private final ExecutorService threadPool;

    public FileDataConnector(FileParserHelper parserHelper, RecordCallbackHandler recordCallbackHandler, File[] input) {
        this.parserHelper = parserHelper;
        this.recordCallbackHandler = recordCallbackHandler;
        this.input = input;
        this.threadPool = Executors.newFixedThreadPool(1);
    }

    @Override
    public boolean init() {
        return true; // nothing to initialize
    }

    @Override
    public void start() {
        logger.info("Starting to process files. File count: " + input.length);
        threadPool.submit(this);
    }

    @Override
    public void terminate() {
        if (!threadPool.isTerminated()) {
            try {
                logger.info("Attempting to terminate the thread pool");
                boolean terminated = threadPool.awaitTermination(5, TimeUnit.SECONDS);
                logger.info("Status of the thread pool termination " + (terminated ? "true" : "false"));
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for thread pool to terminate.", e);
            }
        }
    }

    @Override
    public void run() {
        int processedFileCount = 0;
        for (File f : input) {
            if (!f.isFile()) {
                logger.warn("Skipping " + f.getAbsolutePath() + ", not a file.");
                continue;
            }
            FileReader fileReader = null;
            BufferedReader bufferedReader = null;
            try {
                fileReader = new FileReader(f);
                bufferedReader = new BufferedReader(fileReader);
                int lineCount = 0;
                String line;
                if (parserHelper.skipHeader()) {
                    bufferedReader.readLine(); // skip the header
                }
                while ((line = bufferedReader.readLine()) != null) {
                    Record record = parserHelper.parse(line);
                    recordCallbackHandler.onRecordAvailability(record);
                    lineCount++;
                }
                processedFileCount++;
                logger.info("Completed processing " + f.getAbsolutePath() + ", Total lines: " + lineCount +
                        ", Total files processed: " + processedFileCount);
            } catch (FileNotFoundException e) {
                logger.error("File not found. Path: " + f.getAbsolutePath(), e);
            } catch (IOException e) {
                logger.error("Error reading from file. Path: " + f.getAbsolutePath(), e);
            } finally {
                if (fileReader != null) {
                    try {
                        fileReader.close();
                    } catch (IOException ignore) {
                        // ignore
                    }
                }
                if (bufferedReader != null) {
                    try {
                        bufferedReader.close();
                    } catch (IOException ignore) {
                        // ignore
                    }
                }
            }
        }
        logger.info("Completed processing input. Processed file count: " + processedFileCount);
        recordCallbackHandler.onTermination(); // acknowledge the end of the input
    }
}
