package sustain.synopsis.ingestion.client.connectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.ingestion.client.connectors.file.FileDataConnector;
import sustain.synopsis.ingestion.client.connectors.file.RecordParser;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class FileConnectorTest {

    @Mock
    private RecordCallbackHandler recordCallbackHandler;

    @Mock
    private RecordParser fileParserHelper;

    @TempDir
    Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        Path input = Files.createFile(tempDir.resolve("input.txt"));
        File file = input.toFile();
        FileWriter fw = new FileWriter(file);
        fw.write("geohash, time, f1, f2, f3\n");
        fw.write("9xjm1tneg,1391216400000,1.0, 2.0, 3.0\n");
        fw.write("9xjm1tneg,1391216400001,1.0, 2.0, 3.0\n");
        fw.write("9xjm1tneg,1391216400002,1.0, 2.0, 3.0\n");
        fw.flush();
        fw.close();
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void testFileDataConnector() {
        Mockito.when(fileParserHelper.parse(Mockito.anyString())).thenReturn(new Record());
        Mockito.when(fileParserHelper.skipHeader()).thenReturn(true);
        FileDataConnector connector = new FileDataConnector(fileParserHelper, recordCallbackHandler,
                new File[]{tempDir.resolve("input.txt").toFile()});
        connector.init();
        connector.start();
        long timeout = 15 * 1000; // allow at most 15 seconds to parse the file
        Mockito.verify(fileParserHelper, Mockito.timeout(timeout).times(1)).
                skipHeader();
        Mockito.verify(fileParserHelper, Mockito.timeout(timeout).times(3)).
                parse(Mockito.anyString());
        Mockito.verify(recordCallbackHandler, Mockito.timeout(timeout).times(3)).
                onRecordAvailability(Mockito.any(Record.class));
        Mockito.verify(recordCallbackHandler, Mockito.timeout(timeout).times(1)).
                onTermination();
    }

    @Test
    void testMultipleFiles(){
        Mockito.when(fileParserHelper.parse(Mockito.anyString())).thenReturn(new Record());
        Mockito.when(fileParserHelper.skipHeader()).thenReturn(true);
        // pass the same file twice as input to simulate multiple files
        FileDataConnector connector= new FileDataConnector(fileParserHelper, recordCallbackHandler,
                new File[]{tempDir.resolve("input.txt").toFile(), tempDir.resolve("input.txt").toFile()});
        connector.init();
        connector.start();
        long timeout = 15 * 1000; // allow at most 15 seconds to parse the file
        Mockito.verify(fileParserHelper, Mockito.timeout(timeout).times(6)).
                parse(Mockito.anyString());
    }
}
