package sustain.synopsis.ingestion.client.core;

public interface DataConnector {
    boolean init();
    void start();
    void terminate();
}
