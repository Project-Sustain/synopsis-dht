package sustain.synopsis.ingestion.client.core;

/**
 * Used for asynchronous communication between a {@link DataConnector} and the {@link Driver}.
 * Driver provides an implementation of {@link RecordCallbackHandler} when instantiating a data connector.
 * This allows decoupling between the execution of the data connectors and the driver, for e.g.: a data connector
 * may use multiple threads to fetch data whereas the driver is a single threaded implementation.
 * Also having an asynchronous {@link Record} delivery mechanism can handle various data sources such as storage
 * systems and streams.
 */
public interface RecordCallbackHandler {
    /**
     * Invokes this method if a record is available.
     * @param record {@link Record} object
     */
    public void onRecordAvailability(Record record);

    /**
     * Notifies the end of the dataset. Used by the {@link Driver} to flush any unfinished strand and initiate the
     * cleanup process.
     */
    public void onTermination();
}
