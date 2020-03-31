package sustain.synopsis.dht.store.workers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class WriterPoolTest {

    @Test
    void testPoolInit() {
        final int parallelism = 3;
        WriterPool pool = new WriterPool(parallelism);
        Assertions.assertEquals(parallelism, pool.executors.length);
    }

    @Test
    void testDeterministicTaskAssignment() throws InterruptedException, ExecutionException {
        WriterPool pool = new WriterPool(3);
        CompletableFuture<String> future1 =
                CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), pool.getExecutor(0));
        CompletableFuture<String> future2 =
                CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), pool.getExecutor(0));
        CompletableFuture<String> future3 =
                CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), pool.getExecutor(3));
        Assertions.assertEquals(future1.get(), future2.get());
        Assertions.assertEquals(future1.get(), future3.get());
    }

    @Test
    void testNegativeHashCodes() {
        WriterPool pool = new WriterPool(3);
        CompletableFuture<String> future1 =
                CompletableFuture.supplyAsync(() -> Thread.currentThread().getName(), pool.getExecutor(-1));
        Assertions.assertNotNull(future1);
    }

}
