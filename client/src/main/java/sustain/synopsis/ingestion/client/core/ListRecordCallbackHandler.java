package sustain.synopsis.ingestion.client.core;

import java.util.ArrayList;

public class ListRecordCallbackHandler implements RecordCallbackHandler {

    private ArrayList<Record> records = new ArrayList<>();

    @Override
    public boolean onRecordAvailability(Record record) {
        records.add(record);
        return true;
    }

    @Override
    public void onTermination() {

    }

    public ArrayList<Record> getRecords() {
        return records;
    }
}
