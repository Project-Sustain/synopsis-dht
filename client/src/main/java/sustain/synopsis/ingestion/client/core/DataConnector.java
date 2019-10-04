package sustain.synopsis.ingestion.client.core;

public interface DataConnector {
    public boolean init();
    public void start();
    public void terminate();
}
