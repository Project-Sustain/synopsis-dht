package sustain.synopsis.proxy.ingestion;

import com.google.protobuf.Message;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class ResponseContainer<T extends Message> {

    private class ThreadSafeResponseBase {
        private final Semaphore semaphore = new Semaphore(1);
        private T baseMessage = helper.getEmptyMessage();

        private void merge(T newResponse) {
            try {
                semaphore.acquire();
                baseMessage = helper.merge(baseMessage, newResponse);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                semaphore.release();
            }
        }
    }

    private final AtomicInteger remainingResponseCount;
    private final ResponseMergeHelper<T> helper;
    private final int mergeParallelism;
    private final List<ThreadSafeResponseBase> baseResponses;

    public ResponseContainer(int remainingResponseCount, ResponseMergeHelper<T> helper, int mergeParallelism) {
        this.remainingResponseCount = new AtomicInteger(remainingResponseCount);
        this.helper = helper;
        this.mergeParallelism = mergeParallelism;
        this.baseResponses = new ArrayList<>(mergeParallelism);
        IntStream.range(0, mergeParallelism).forEach(i -> this.baseResponses.add(new ThreadSafeResponseBase()));
    }

    public boolean add(T resp) {
        ThreadSafeResponseBase base = baseResponses.get(remainingResponseCount.get() % mergeParallelism);
        base.merge(resp);
        return remainingResponseCount.decrementAndGet() == 0;
    }

    public T getMergedResponse() {
        T msg1 = baseResponses.get(0).baseMessage;
        for (int i = 1; i < mergeParallelism; i++) {
            msg1 = helper.merge(msg1, baseResponses.get(i).baseMessage);
        }
        return msg1;
    }
}
