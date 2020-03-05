package sustain.synopsis.ingestion.client.connectors;

public interface DataConnector {
    boolean init();
    void start();
    void terminate();
}
