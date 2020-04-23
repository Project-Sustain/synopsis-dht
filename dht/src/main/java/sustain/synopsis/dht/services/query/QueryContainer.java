package sustain.synopsis.dht.services.query;

import io.grpc.stub.StreamObserver;
import org.apache.log4j.Logger;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.Queue;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryContainer implements Runnable {
    private final Logger logger = Logger.getLogger(QueryContainer.class);
    private AtomicBoolean queryActive = new AtomicBoolean(true);
    private final CountDownLatch latch;
    private final CompletableFuture<Boolean> future;
    private final StreamObserver<TargetQueryResponse> responseObserver;
    private Thread streamWriter;
    private final Queue<EntityStore> pendingTasks;
    private final BlockingQueue<TargetQueryResponse> queue;

    public QueryContainer(CountDownLatch latch, CompletableFuture<Boolean> future,
                          StreamObserver<TargetQueryResponse> responseObserver, Set<EntityStore> entityStores,
                          int bufferCapacity) {
        this.latch = latch;
        this.future = future;
        this.responseObserver = responseObserver;
        this.streamWriter = new Thread(this);
        this.pendingTasks = new ConcurrentLinkedQueue<>(entityStores);
        this.queue = new LinkedBlockingDeque<>(bufferCapacity);
    }

    public EntityStore getNextTask() {
        return queryActive.get() ? pendingTasks.poll() : null;
    }

    public void write(TargetQueryResponse targetQueryResponse) {
        // if the query is terminated, stop accepting responses. Otherwise, the all writers may get blocked
        // because the consumer thread has stopped.
        if (!queryActive.get()) {
            return;
        }
        try {
            queue.put(targetQueryResponse);
        } catch (InterruptedException e) {
            logger.error("Error during writing.", e);
            streamWriter.interrupt(); // make the query terminate
        }
    }

    public void reportReaderTaskComplete(boolean successful) {
        if (!successful) {
            streamWriter.interrupt();
            return;
        }
        latch.countDown();
        if (logger.isDebugEnabled() && latch.getCount() == 0) {
            logger.debug("Last reader task is done. Disk I/O is complete.");
        }
    }

    @Override
    public void run() {
        long startTS = System.nanoTime();
        while (latch.getCount() > 0 || !queue.isEmpty()) {
            try {
                if (Thread.interrupted()) {
                    throw new Exception("A reader task has failed.");
                }
                TargetQueryResponse resp = queue.poll(3, TimeUnit.SECONDS);
                if (resp != null) {
                    responseObserver.onNext(resp);
                }
            } catch (Throwable e) {
                // Could be due to errors when writing to the stream or a reader task failure.
                logger.error("Query Evaluation Error.", e);
                queryActive.set(false);
                queue.clear(); // this will unlock any waiting writers
                future.complete(false);
                return;
            }
        }
        future.complete(true);
        if (logger.isDebugEnabled()) {
            logger.debug("Publishing to stream is complete. Time elapsed (ms): " + (System.nanoTime() - startTS) / Math
                    .pow(10d, 6));
        }
    }

    public void startStreamPublisher() {
        this.streamWriter.start();
    }
}
