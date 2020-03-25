package sustain.synopsis.dht;

import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.StrandStorageValue;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.NodeMapping;
import sustain.synopsis.dht.store.services.Strand;
import sustain.synopsis.dht.store.workers.WriterPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class IngestionRequestDispatcher {
    private final WriterPool writerPool;
    private final NodeStore nodeStore;

    public IngestionRequestDispatcher(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
        this.writerPool = new WriterPool(Context.getInstance().getNodeConfig().getWriterPoolSize());
    }

    public CompletableFuture<NodeMapping> getNodeMappingCompletableFuture(String datasetId, long sessionId, Strand strand) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                nodeStore.store(
                        datasetId,
                        strand.getEntityId(),
                        sessionId,
                        new StrandStorageKey(strand.getFromTs(), strand.getToTs()),
                        new StrandStorageValue(strand.getBytes().toByteArray())
                );
            } catch (IOException | StorageException e) {
                e.printStackTrace();
            }
            return NodeMapping.newBuilder().build();
        }, writerPool.getExecutor(strand.getEntityId().hashCode()));
    }

    public CompletableFuture<IngestionResponse> toCombinedCompletableFuture(String datasetId, long sessionId, List<CompletableFuture<NodeMapping>> nodeMappingCompletableFutures) {
        CompletableFuture<List<NodeMapping>> allCompletableFuture =
                CompletableFuture.allOf(nodeMappingCompletableFutures.toArray(new CompletableFuture[nodeMappingCompletableFutures.size()]))
                        .thenApply(future -> {
                            return nodeMappingCompletableFutures.stream()
                                    .map(CompletableFuture::join)
                                    .collect(Collectors.toList());
        });

        return allCompletableFuture.thenApply(nodeMappings -> {
            return IngestionResponse.newBuilder()
                    .setDatasetId(datasetId)
                    .setSessionId(sessionId)
                    .setStatus(true)
                    .addAllMapping(nodeMappings.stream()
                            .filter(mapping -> mapping.getDhtNodeAddress() != null)
                            .collect(Collectors.toList())
                    )
                    .build();
        });
    }

    public CompletableFuture<IngestionResponse> dispatch(IngestionRequest ingestionRequest) {
        List<CompletableFuture<NodeMapping>> nodeMappingCompletableFutures = new ArrayList<>(ingestionRequest.getStrandCount());
        for (int i = 0; i < nodeMappingCompletableFutures.size(); i++) {
            nodeMappingCompletableFutures.set(i, getNodeMappingCompletableFuture(
                    ingestionRequest.getDatasetId(),
                    ingestionRequest.getSessionId(),
                    ingestionRequest.getStrand(i))
            );
        }

        return toCombinedCompletableFuture(
                ingestionRequest.getDatasetId(),
                ingestionRequest.getSessionId(),
                nodeMappingCompletableFutures
        );
    }

}
