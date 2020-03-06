package sustain.synopsis.ingestion.client.connectors.file;

import org.apache.log4j.Logger;
import sustain.synopsis.ingestion.client.connectors.DataConnector;
import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A data connector to read records from a list of files. '\n' character is used to separate the records.
 * Each file is processed using the provided {@link FileParser}.
 * A single threaded executor is used to run the data connector.
 */
public class FileDataConnector implements DataConnector, Runnable {
    private final Logger logger = Logger.getLogger(FileDataConnector.class);
    private final FileParser fileParser;
    private final File[] input;
    private final ExecutorService threadPool;
    private RecordCallbackHandler handler;

    public FileDataConnector(FileParser fileParser, File[] input) {
        this.fileParser = fileParser;
        this.input = input;
        this.threadPool = Executors.newFixedThreadPool(1);
    }

    @Override
    public boolean initWithIngestionConfigAndRecordCallBackHandler(SessionSchema config, RecordCallbackHandler handler) {
        this.fileParser.initWithSchemaAndHandler(config, handler);
        this.handler = handler;
        return true;
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
        for (File file : input) {
            if (!file.isFile()) {
                logger.warn("Skipping " + file.getAbsolutePath() + ", not a file.");
                continue;
            }
            fileParser.parse(file);
            processedFileCount++;
        }
        logger.info("Completed processing input. Processed file count: " + processedFileCount);
        handler.onTermination(); // acknowledge the end of the input
    }
}
