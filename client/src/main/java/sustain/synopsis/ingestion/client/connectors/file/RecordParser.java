package sustain.synopsis.ingestion.client.connectors.file;

import sustain.synopsis.ingestion.client.core.Record;

/**
 * Encapsulates the parsing logic to convert the text line into a record. Used by the {@link FileDataConnector}.
 */
public interface RecordParser {
    /**
     * Parse an individual line of text and create a record with the appropriate fields.
     * @param line Line provided by the {@link FileDataConnector}
     * @return {@link Record} object
     */
    Record parse(String line, int lineNum);
}
