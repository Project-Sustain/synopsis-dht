package sustain.synopsis.ingestion.client.connectors;

import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

public interface DataConnector {
    boolean initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler callbackHandler);
    void start();
    void terminate();
}
