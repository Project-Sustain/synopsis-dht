package sustain.synopsis.dht.store.workers;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Encapsulates a set of writer tasks responsible for writing all ingested data for all entities
 * in the purview of the current node.
 */
public class WriterPool {

    private Logger logger = Logger.getLogger(WriterPool.class);

    /**
     * Implements single writer task responsible for a set of entities.
     */
    class Writer implements Runnable{
        private final int index;
        private CountDownLatch latch;

        public Writer(int index, CountDownLatch latch) {
            this.index = index;
            this.latch = latch;
        }

        @Override
        public void run() {
            BlockingQueue<WriteTask> queue = pendingTasks.get(index);
            latch.countDown();
            if(logger.isDebugEnabled()){
                logger.debug("Writer task " + index + " is ready.");
            }
            // todo: introduce graceful shutdown support
            while (true){
                try {
                    WriteTask task = queue.take();
                    task.write();
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for a write task. WriterThread id: " + index);
                } catch (Throwable e){
                    // keep the thread alive in the face of runtime errors.
                    logger.error("Error in writer task. ", e);
                }
            }
        }
    }

    /**
     * Number of writer tasks for a storage node
     */
    private final int parallelism;

    final ExecutorService executors; // package-local access for unit testing
    final List<BlockingQueue<WriteTask>> pendingTasks; // package-local access for unit testing

    public WriterPool(int parallelism) {
        this.parallelism = parallelism;
        this.executors = Executors.newFixedThreadPool(parallelism);
        this.pendingTasks = new ArrayList<>(parallelism);
    }

    public void init(){
        CountDownLatch latch = new CountDownLatch(parallelism);
        for(int i = 0; i < parallelism; i++){
            // todo: what is the capacity here? how to gracefully handle high traffic?
            this.pendingTasks.add(i, new ArrayBlockingQueue<>(1024));
            Writer writer = new Writer(i, latch);
            this.executors.submit(writer);
        }
        try {
            latch.await();
            logger.info("Writer pool is ready. Parallelism : " + parallelism);
        } catch (InterruptedException e) {
            logger.error("Error awaiting for the countdown latch to complete.", e);
        }
    }

    public boolean write(WriteTask writeTask){
        int index = writeTask.hashCode() % parallelism;
        return pendingTasks.get(index).offer(writeTask);
    }
}
