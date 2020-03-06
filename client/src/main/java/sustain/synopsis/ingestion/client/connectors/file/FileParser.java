package sustain.synopsis.ingestion.client.connectors.file;

import sustain.synopsis.ingestion.client.core.SessionSchema;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.File;

public interface FileParser {
    public void initWithSchemaAndHandler(SessionSchema schema, RecordCallbackHandler handler);
    public void parse(File file);

}
