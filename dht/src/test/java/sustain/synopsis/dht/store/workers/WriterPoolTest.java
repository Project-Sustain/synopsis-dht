package sustain.synopsis.dht.store.workers;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WriterPoolTest {

    private class FakeWrite extends WriteTask{
        private long execThreadId;
        private final int identifier;

        private FakeWrite(int identifier) {
            this.identifier = identifier;
        }

        @Override
        public void write() {
            execThreadId = Thread.currentThread().getId();
        }

        @Override
        public int getIdentifier() {
            return identifier;
        }
    }

    @Test
    public void testPoolInit(){
        final int parallelism = 3;
        WriterPool pool = new WriterPool(parallelism);
        pool.init();
        Assertions.assertEquals(parallelism, pool.pendingTasks.size());
    }

    @Test
    public void testDeterministicTaskAssignment(){
        WriterPool pool = new WriterPool(3);
        pool.init();
        FakeWrite task1 = new FakeWrite(1);
        FakeWrite task2 = new FakeWrite(1);
        pool.write(task1);
        pool.write(task2);
        Assertions.assertEquals(task1.execThreadId, task2.execThreadId);
    }
}
