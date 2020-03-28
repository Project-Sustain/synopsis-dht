package sustain.synopsis.dht.store.query;

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;

public class QueryCoordinator {
    private final Logger logger = Logger.getLogger(QueryContainer.class);
    private final NodeStore nodeStore;
    private final ExecutorService readers;

    public QueryCoordinator(NodeStore nodeStore, ExecutorService readers) {
        this.nodeStore = nodeStore;
        this.readers = readers;
    }

    public CompletableFuture<Boolean> schedule(TargetQueryRequest queryRequest,
                                               StreamObserver<TargetQueryResponse> responseObserver) throws QueryException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        Set<EntityStore> matchingEntityStores = nodeStore.getMatchingEntityStores(queryRequest);
        if(logger.isDebugEnabled()){
            logger.debug("Number of matching entity stores: " + matchingEntityStores.size());
        }
        if (matchingEntityStores.isEmpty()) {
            future.complete(false);
            return future;
        }
        QueryContainer container = new QueryContainer(new CountDownLatch(matchingEntityStores.size()), future,
                responseObserver);
        for (EntityStore entityStore : matchingEntityStores) {
            readers.submit(new ReaderTask(entityStore, queryRequest, container));
        }
        return future;
    }
}
