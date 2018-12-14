package synopsis2.dht;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Thilina Buddhika
 */
public class WorkerPool {

    private static final WorkerPool instance = new WorkerPool();
    private final ExecutorService pool;
    private AtomicLong remaining = new AtomicLong(0);

    public WorkerPool() {
        this.pool = Executors.newFixedThreadPool(Util.getWorkerPoolSize());
    }

    public static WorkerPool getInstance() {
        return instance;
    }

    public void submit(Runnable task) {
        pool.submit(task);
        remaining.incrementAndGet();
    }

    public void notifyCompleted() {
        long remCount = remaining.decrementAndGet();
        //System.out.println("remaining task count: " + remCount);
    }
}
