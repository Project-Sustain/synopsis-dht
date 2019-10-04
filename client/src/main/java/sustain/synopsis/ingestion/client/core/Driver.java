package sustain.synopsis.ingestion.client.core;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Driver implements RecordCallbackHandler{

    private BlockingQueue<Record> queue = new ArrayBlockingQueue<>(5);

    @Override
    public void onRecordAvailability(Record record) {
        queue.add(record);
    }

    @Override
    public void onTermination() {

    }
}
