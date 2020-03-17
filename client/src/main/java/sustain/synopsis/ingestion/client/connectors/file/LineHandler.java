package sustain.synopsis.ingestion.client.connectors.file;

import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.SessionSchema;

import java.util.Map;

public interface LineHandler {

    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler recordCallbackHandler);
    public void onDataAvailability(Map<String, Integer> columnMap, String[] splits);

}
