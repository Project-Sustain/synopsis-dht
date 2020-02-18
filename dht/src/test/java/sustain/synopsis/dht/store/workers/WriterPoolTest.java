package sustain.synopsis.dht.store.workers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class WriterPoolTest {

    private static class FakeWrite extends WriteTask {
        private AtomicReference<String> execThreadId = new AtomicReference<>("");
        private final int identifier;
        private final CountDownLatch latch;

        private FakeWrite(int identifier, CountDownLatch latch) {
            this.identifier = identifier;
            this.latch = latch;
        }

        @Override
        public void run() {
            execThreadId.set(Thread.currentThread().getName());
            latch.countDown();
        }

        @Override
        public int getIdentifier() {
            return identifier;
        }
    }

    @Test
    public void testPoolInit() {
        final int parallelism = 3;
        WriterPool pool = new WriterPool(parallelism);
        Assertions.assertEquals(parallelism, pool.executors.length);
    }

    @Test
    public void testDeterministicTaskAssignment() throws InterruptedException {
        WriterPool pool = new WriterPool(3);
        CountDownLatch latch = new CountDownLatch(4);
        FakeWrite task1 = new FakeWrite(1, latch);
        FakeWrite task2 = new FakeWrite(1, latch);
        FakeWrite task3 = new FakeWrite(2, latch);
        FakeWrite task4 = new FakeWrite(3, latch);
        pool.getExecutor(task1).submit(task1);
        pool.getExecutor(task2).submit(task2);
        pool.getExecutor(task3).submit(task3);
        pool.getExecutor(task4).submit(task4);
        latch.await();
        Assertions.assertEquals("writer-1", task1.execThreadId.get());
        Assertions.assertEquals("writer-1", task2.execThreadId.get());
        Assertions.assertEquals("writer-2", task3.execThreadId.get());
        Assertions.assertEquals("writer-0", task4.execThreadId.get());
    }
}
