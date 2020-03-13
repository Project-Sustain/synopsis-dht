package sustain.synopsis.dht;

import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.StrandStorageValue;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.*;
import sustain.synopsis.dht.store.workers.WriterPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

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
        CompletableFuture<NodeMapping>[] nodeMappingCompletableFutures = new CompletableFuture[ingestionRequest.getEntityCount()];
        for (int i = 0; i < nodeMappingCompletableFutures.length; i++) {
            Entity entity = ingestionRequest.getEntity(i);
            nodeMappingCompletableFutures[i] = CompletableFuture.supplyAsync(() -> {
                try {
                    nodeStore.store(
                            ingestionRequest.getDatasetId(),
                            entity.getEntityId(),
                            ingestionRequest.getSessionId(),
                            new StrandStorageKey(entity.getFromTs(), entity.getToTs()),
                            new StrandStorageValue(entity.getBytes().toByteArray())
                    );
                } catch (IOException | StorageException e) {
                    e.printStackTrace();
                }
                return NodeMapping.newBuilder().build();
            }, writerPool.getExecutor(entity.getEntityId().hashCode()));
        }

        CompletableFuture<List<NodeMapping>> allCompletableFuture = CompletableFuture.allOf(nodeMappingCompletableFutures).thenApply(future -> {
            return Arrays.stream(nodeMappingCompletableFutures)
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList());
        });

        return allCompletableFuture.thenApply(nodeMappings -> {
            return IngestionResponse.newBuilder()
                    .setSessionId(ingestionRequest.getSessionId())
                    .setStatus(true)
                    .addAllMapping(nodeMappings.stream()
                            .filter(mapping -> mapping.getDhtNodeAddress() != null)
                            .collect(Collectors.toList())
                    )
                    .build();
        });
    }

}
