package sustain.synopsis.dht.services.query;

import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.TargetQueryRequest;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class DHTQueryProcessor implements QueryProcessor {
    private static final int QUERY_CONTAINER_BUFFER_SIZE = 1024;
    private final Logger logger = Logger.getLogger(DHTQueryProcessor.class);
    private final NodeStore nodeStore;
    private final ExecutorService readers;
    private int readerCountPerQuery = Runtime.getRuntime().availableProcessors();

    public DHTQueryProcessor(NodeStore nodeStore) {
        this.nodeStore = nodeStore;
        this.readers = Executors.newFixedThreadPool(readerCountPerQuery * 2);
    }

    // used for unit testing with mock injection
    public DHTQueryProcessor(NodeStore nodeStore, ExecutorService readers, int defaultReaderCount) {
        this.nodeStore = nodeStore;
        this.readerCountPerQuery = defaultReaderCount;
        this.readers = readers;
    }

    public CompletableFuture<Boolean> process(TargetQueryRequest queryRequest, QueryResponseHandler responseHandler) {
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        Set<EntityStore> matchingEntityStores = nodeStore.getMatchingEntityStores(queryRequest);
        if (logger.isDebugEnabled()) {
            logger.debug("Number of matching entity stores: " + matchingEntityStores.size());
        }
        if (matchingEntityStores.isEmpty()) {
            future.complete(true);
            return future;
        }
        int readerTaskCount = Math.min(matchingEntityStores.size(), readerCountPerQuery);
        QueryContainer container =
                new QueryContainer(new CountDownLatch(readerTaskCount), future, responseHandler, matchingEntityStores,
                                   QUERY_CONTAINER_BUFFER_SIZE);
        container.startStreamPublisher();
        IntStream.range(0, readerTaskCount).forEach(i -> readers.submit(new ReaderTask(queryRequest, container)));
        return future;
    }
}
