package sustain.synopsis.ingestion.client.connectors.file;

import sustain.synopsis.ingestion.client.core.Record;

/**
 * Encapsulates the parsing logic to convert the text line into a record. Used by the {@link FileDataConnector}.
 */
public interface FileParserHelper {
    /**
     * Parse an individual line of text and create a record with the appropriate fields.
     * @param line Line provided by the {@link FileDataConnector}
     * @return {@link Record} object
     */
    public Record parse(String line);

    /**
     * Whether to skip the first line in a file, which can be a header especially in a CSV
     * @return <code>true</code> if the first line should be skipped from parsing
     */
    public boolean skipHeader();
}
