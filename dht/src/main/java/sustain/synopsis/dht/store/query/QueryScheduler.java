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
import java.util.stream.IntStream;

public class QueryScheduler {
    private static final int QUERY_CONTAINER_BUFFER_SIZE = 1024;
    private final Logger logger = Logger.getLogger(QueryScheduler.class);
    private final NodeStore nodeStore;
    private final ExecutorService readers;

    public QueryScheduler(NodeStore nodeStore, ExecutorService readers) {
        this.nodeStore = nodeStore;
        this.readers = readers;
    }

    public CompletableFuture<Boolean> schedule(TargetQueryRequest queryRequest,
                                               StreamObserver<TargetQueryResponse> responseObserver, int readerCount)
            throws QueryException {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        Set<EntityStore> matchingEntityStores = nodeStore.getMatchingEntityStores(queryRequest);
        if (logger.isDebugEnabled()) {
            logger.debug("Number of matching entity stores: " + matchingEntityStores.size());
        }
        if (matchingEntityStores.isEmpty()) {
            future.complete(true);
            return future;
        }
        QueryContainer container =
                new QueryContainer(new CountDownLatch(readerCount), future, responseObserver, matchingEntityStores,
                                   QUERY_CONTAINER_BUFFER_SIZE);
        container.startStreamPublisher();
        IntStream.range(0, readerCount).forEach(i -> readers.submit(new ReaderTask(queryRequest, container)));
        return future;
    }
}
