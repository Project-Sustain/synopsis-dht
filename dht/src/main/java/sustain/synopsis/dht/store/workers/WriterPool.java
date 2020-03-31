package sustain.synopsis.dht.store.workers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * Implements thread pool to facilitate the single writer I/O model.
 */
public class WriterPool {
    final ExecutorService[] executors;
    private final int parallelism;

    public WriterPool(int parallelism) {
        this.parallelism = parallelism;
        this.executors = new ExecutorService[parallelism];
        // initialize each executor.
        for (int i = 0; i < parallelism; i++) {
            int threadId = i;
            executors[i] = Executors.newFixedThreadPool(1, r -> new Thread(r, "writer-" + threadId));
        }
    }

    public ExecutorService getExecutor(int hash) {
        return executors[Math.abs(hash) % parallelism];
    }
}
