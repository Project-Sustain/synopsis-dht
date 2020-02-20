package sustain.synopsis.dht.store.workers;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


/**
 * Implements thread pool to facilitate the single writer I/O model.
 */
public class WriterPool {
    private final int parallelism;
    final ExecutorService[] executors;

    public WriterPool(int parallelism) {
        this.parallelism = parallelism;
        this.executors = new ExecutorService[parallelism];
        // initialize each executor.
        for (int i = 0; i < parallelism; i++) {
            int threadId = i;
            executors[i] = Executors.newFixedThreadPool(1, new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "writer-" + threadId);
                }
            });
        }
    }

    public ExecutorService getExecutor(int hash) {
        return executors[hash % parallelism];
    }
}
