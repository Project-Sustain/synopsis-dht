package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.ingestion.client.connectors.DataConnector;

public class IngestionConfiguration {
    private DataConnector dataConnector;
    private IngestionTaskManager ingestionTaskManager;

    public IngestionConfiguration() {}

    public IngestionConfiguration(DataConnector dataConnector, IngestionTaskManager ingestionTaskManager) {
        this.dataConnector = dataConnector;
        this.ingestionTaskManager = ingestionTaskManager;
    }

    public DataConnector getDataConnector() {
        return dataConnector;
    }

    public IngestionTaskManager getIngestionTaskManager() {
        return ingestionTaskManager;
    }

}
