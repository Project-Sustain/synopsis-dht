package sustain.synopsis.ingestion.client.connectors.file;

import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.connectors.DataConnector;
import sustain.synopsis.ingestion.client.connectors.DataParser;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A data connector to read records from a list of files. '\n' character is used to separate the records.
 * Each line is converted to a record using the provided {@link RecordParser}.
 * A single threaded executor is used to run the data connector.
 */
public class FileDataConnector implements DataConnector, Runnable {
    private final Logger logger = Logger.getLogger(FileDataConnector.class);
    private final DataParser fileParser;
    private final RecordCallbackHandler recordCallbackHandler;
    private final File[] input;
    private final ExecutorService threadPool;
    private int processedFileCount;

    public FileDataConnector(DataParser fileParser, RecordCallbackHandler recordCallbackHandler, File[] input) {
        this.fileParser = fileParser;
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


    private void readFile(File f) {
        FileReader fileReader = null;
        BufferedReader bufferedReader = null;
        try {
            fileReader = new FileReader(f);
            bufferedReader = new BufferedReader(fileReader);

            fileParser.parseFromReaderWithHandler(bufferedReader, recordCallbackHandler);

            processedFileCount++;
            logger.info("Completed processing " + f.getAbsolutePath() +
                    ", Total files processed: " + processedFileCount);
        } catch (FileNotFoundException e) {
            logger.error("File not found. Path: " + f.getAbsolutePath(), e);
        } catch (IOException e) {
            logger.error("Error reading from file. Path: " + f.getAbsolutePath(), e);
        } catch (Throwable e){
            logger.error("Error during execution.", e);
        }
        finally {
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

    @Override
    public void run() {
        processedFileCount = 0;
        for (File f : input) {
            if (!f.isFile()) {
                logger.warn("Skipping " + f.getAbsolutePath() + ", not a file.");
                continue;
            }
            readFile(f);
        }
        logger.info("Completed processing input. Processed file count: " + processedFileCount);
        recordCallbackHandler.onTermination(); // acknowledge the end of the input
    }
}
