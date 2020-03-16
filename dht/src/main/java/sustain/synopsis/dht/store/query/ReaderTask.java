package sustain.synopsis.dht.store.query;

import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.TargetQueryRequest;

public class ReaderTask implements Runnable {

    private final EntityStore entityStore;
    private final TargetQueryRequest queryRequest;
    private final QueryContainer container;

    public ReaderTask(EntityStore entityStore, TargetQueryRequest queryRequest, QueryContainer container) {
        this.entityStore = entityStore;
        this.queryRequest = queryRequest;
        this.container = container;
    }

    @Override
    public void run() {

    }
}
