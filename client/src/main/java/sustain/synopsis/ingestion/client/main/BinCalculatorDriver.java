package sustain.synopsis.ingestion.client.main;
import sustain.synopsis.ingestion.client.connectors.file.FileParser;
import sustain.synopsis.ingestion.client.core.BinCalculator;
import sustain.synopsis.ingestion.client.core.Record;
import sustain.synopsis.ingestion.client.core.RecordCallbackHandler;
import sustain.synopsis.ingestion.client.core.Util;

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

        MyRecordCallbackHandler handler = new MyRecordCallbackHandler();
        fileParser.initWithSchemaAndHandler(null, handler);
        for (File file : files) {
            fileParser.parse(file);
        }

        String binConfiguration = new BinCalculator().getBinConfiguration(handler.records);

        System.out.println(binConfiguration);
    }

    private static class MyRecordCallbackHandler implements RecordCallbackHandler {
        ArrayList<Record> records = new ArrayList<>();

        @Override
        public boolean onRecordAvailability(Record record) {
            records.add(record);
            return true;
        }

        @Override
        public void onTermination() {

        }
    }

}
