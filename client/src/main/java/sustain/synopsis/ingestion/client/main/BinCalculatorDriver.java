package sustain.synopsis.ingestion.client.main;
import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.*;

import java.io.File;
import java.util.ArrayList;

public class BinCalculatorDriver {

    public static void main(String[] args) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        if (args.length < 2) {
            System.out.println("usage: FileParserClassName files...");
            return;
        }

        String fileParserClassName = args[0];
        FileParser fileParser = (FileParser) Class.forName(fileParserClassName).newInstance();
        File[] files = Util.getFilesFromStrings(1, args);

        ListRecordCallbackHandler handler = new ListRecordCallbackHandler();
        fileParser.initWithSchemaAndHandler(null, handler);
        for (File file : files) {
            fileParser.parse(file);
        }

        String binConfiguration = new BinCalculator().getBinConfiguration(handler.getRecords());

        System.out.println(binConfiguration);
    }

}
