package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.ingestion.client.connectors.DataConnector;
import synopsis2.client.IngestionConfig;

public class Client {

    DataConnector dataConnector;
    IngestionTaskManager ingestionTaskManager;

    public void ingest() {
        ingestionTaskManager.start();
        dataConnector.init();
        dataConnector.start();
        ingestionTaskManager.awaitCompletion();
        dataConnector.terminate();
    }


    public IngestionConfig fetchIngestionConfig(String datasetId, long sessionId) {
        return null;
    }


    public static void main(String[] args) throws ClassNotFoundException {

    }

}
