package sustain.synopsis.dht.services.ingestion;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.StrandStorageValue;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.Strand;
import sustain.synopsis.dht.store.services.TerminateSessionRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class IngestionRequestProcessor {
    private final Logger logger = Logger.getLogger(IngestionRequestProcessor.class);
    private final WriterPool writerPool;
    private final NodeStore nodeStore;

    public IngestionRequestProcessor(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
        this.writerPool = new WriterPool(Context.getInstance().getNodeConfig().getWriterPoolSize());
    }

    public CompletableFuture<Boolean> storeStrand(String datasetId, long sessionId, Strand strand) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                nodeStore.store(datasetId, strand.getEntityId(), sessionId,
                                new StrandStorageKey(strand.getFromTs(), strand.getToTs()),
                                new StrandStorageValue(strand.getBytes().toByteArray()));
            } catch (IOException | StorageException e) {
                logger.error("Error while processing strand storage request", e);
                return false;
            }
            return true;
        }, writerPool.getExecutor(strand.getEntityId().hashCode()));
    }

    public CompletableFuture<IngestionResponse> toCombinedCompletableFuture(long messageId, String datasetId,
                                                                            long sessionId,
                                                                            List<CompletableFuture<Boolean>> strandStoreStatus) {
        CompletableFuture<Boolean> statusFuture = combineCompletableBinaryFutures(strandStoreStatus);
        return statusFuture.thenApply(
                status -> IngestionResponse.newBuilder().setMessageId(messageId).setDatasetId(datasetId)
                                           .setSessionId(sessionId).setStatus(status).build());
    }

    public CompletableFuture<IngestionResponse> process(IngestionRequest ingestionRequest) {
        List<CompletableFuture<Boolean>> strandStoreStatus = new ArrayList<>(ingestionRequest.getStrandCount());
        for (int i = 0; i < ingestionRequest.getStrandCount(); i++) {
            strandStoreStatus.add(i, storeStrand(ingestionRequest.getDatasetId(), ingestionRequest.getSessionId(),
                                                 ingestionRequest.getStrand(i)));
        }
        return toCombinedCompletableFuture(ingestionRequest.getMessageId(), ingestionRequest.getDatasetId(),
                                           ingestionRequest.getSessionId(), strandStoreStatus);
    }

    public CompletableFuture<Boolean> terminateSession(TerminateSessionRequest terminateSessionRequest) {
        List<CompletableFuture<Boolean>> futures = nodeStore
                .endSession(terminateSessionRequest.getDatasetId(), terminateSessionRequest.getSessionId(), writerPool);
        return combineCompletableBinaryFutures(futures);
    }

    private CompletableFuture<Boolean> combineCompletableBinaryFutures(
            List<CompletableFuture<Boolean>> strandStoreStatus) {
        return CompletableFuture.allOf(strandStoreStatus.toArray(new CompletableFuture[0])).thenApply(
                future -> strandStoreStatus.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2));
    }

}
