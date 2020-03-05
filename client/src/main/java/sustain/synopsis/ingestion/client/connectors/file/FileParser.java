package sustain.synopsis.ingestion.client.connectors.file;

import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;

public interface FileParser {

    public void parseFromReaderWithHandler(BufferedReader br, RecordCallbackHandler handler) throws IOException;

}
