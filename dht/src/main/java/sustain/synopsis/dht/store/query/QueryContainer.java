package sustain.synopsis.dht.store.query;

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

public class QueryContainer {
    private final Logger logger = Logger.getLogger(QueryContainer.class);
    private final CountDownLatch latch;
    private final CompletableFuture<Boolean> future;
    private final StreamObserver<TargetQueryResponse> responseObserver;

    public QueryContainer(CountDownLatch latch, CompletableFuture<Boolean> future,
                          StreamObserver<TargetQueryResponse> responseObserver) {
        this.latch = latch;
        this.future = future;
        this.responseObserver = responseObserver;
    }

    public void write(TargetQueryResponse targetQueryResponse) {
        synchronized (responseObserver) {
            responseObserver.onNext(targetQueryResponse);
        }
    }

    public void complete() {
        latch.countDown();
        if (latch.getCount() == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("Last reader task is done. Query is complete.");
            }
            future.complete(true);
        }
    }
}
