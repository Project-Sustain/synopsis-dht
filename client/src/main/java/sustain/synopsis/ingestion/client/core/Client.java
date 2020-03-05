package sustain.synopsis.ingestion.client.core;

public class Client {

    public static void ingest(IngestionConfiguration configuration) {
        configuration.getIngestionTaskManager().start();
        configuration.getDataConnector().init();
        configuration.getDataConnector().start();
        configuration.getIngestionTaskManager().awaitCompletion();
        configuration.getDataConnector().terminate();
    }

    public static void main(String[] args) throws ClassNotFoundException {

    }

}
