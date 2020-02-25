package sustain.synopsis.dht;

import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.StrandStorageValue;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.workers.WriterPool;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class IngestionRequestDispatcher {
    private final WriterPool writerPool;
    private final NodeStore nodeStore;

    public IngestionRequestDispatcher() throws StorageException {
        this(new NodeStore(), Context.getInstance().getNodeConfig().getWriterPoolSize());
    }

    // used for unit testing with injected mocks
    public IngestionRequestDispatcher(NodeStore nodeStore, int writerPoolSize) throws StorageException {
        this.nodeStore = nodeStore;
        nodeStore.init();
        this.writerPool = new WriterPool(writerPoolSize);
    }

    public CompletableFuture<IngestionResponse> dispatch(IngestionRequest ingestionRequest) {
        return CompletableFuture.supplyAsync(() -> {
            boolean status = false;
            try {
                nodeStore.store(ingestionRequest.getDatasetId(), ingestionRequest.getEntityId(),
                        ingestionRequest.getSessionId(), new StrandStorageKey(ingestionRequest.getFromTS(),
                                ingestionRequest.getToTS()),
                        new StrandStorageValue(ingestionRequest.getStrand().toByteArray()));
                status = true;
            } catch (IOException | StorageException e) {
                e.printStackTrace();
            }
            return IngestionResponse.newBuilder().setSessionId(ingestionRequest.getSessionId()).setStatus(status).build();
        }, writerPool.getExecutor(ingestionRequest.getEntityId().hashCode()));
    }

}
