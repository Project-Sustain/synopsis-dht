package sustain.synopsis.dht.services.query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.timeout;

public class QueryContainerTest {
    @Mock
    QueryResponseHandler responseHandlerMock;

    @Mock
    EntityStore entityStore1;


    @Test
    void testGetNextTask() throws ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        Set<EntityStore> entityStores = new HashSet<>(Collections.singletonList(entityStore1));
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        QueryContainer container =
                new QueryContainer(new CountDownLatch(1), future, responseHandlerMock, entityStores, 32);
        Assertions.assertEquals(entityStore1, container.getNextTask());
        Assertions.assertNull(container.getNextTask());

        // if there is an error, no new tasks should be issued.
        future = new CompletableFuture<>();
        container = new QueryContainer(new CountDownLatch(1), future, responseHandlerMock, entityStores, 32);
        container.startStreamPublisher();
        TargetQueryResponse response = TargetQueryResponse.newBuilder().build();
        container.write(response);
        // this makes sure that the container thread has started
        Mockito.verify(responseHandlerMock, timeout(60000).times(1)).handleResponse(response);
        container.reportReaderTaskComplete(false);
        Assertions.assertFalse(future.get());
        Assertions.assertNull(container.getNextTask());
    }

    @Test
    void testWrite() {
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        QueryContainer container =
                new QueryContainer(new CountDownLatch(1), future, responseHandlerMock, new HashSet<>(), 32);
        container.startStreamPublisher();
        TargetQueryResponse response = TargetQueryResponse.newBuilder().build();
        container.write(response);
        Mockito.verify(responseHandlerMock, timeout(60000).times(1)).handleResponse(response);
    }

    @Test
    void testWriteWithError() throws ExecutionException, InterruptedException {
        // reader is interrupted before the starting the consumer
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        // buffer size is set to 1
        QueryContainer container =
                new QueryContainer(new CountDownLatch(1), future, responseHandlerMock, new HashSet<>(), 1);
        // now the buffer is full
        container.write(TargetQueryResponse.newBuilder().build());
        // following thread is blocked because the consumer thread has not started
        CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(() -> {
            latch.countDown();
            container.write(TargetQueryResponse.newBuilder().build());
        });
        t.start();
        latch.await();
        // interrupt the thread so that it throws an exception
        t.interrupt();
        container.startStreamPublisher();
        Assertions.assertFalse(future.get());
    }

    @Test
    void testWriteWithError2() throws ExecutionException, InterruptedException {
        // reader is interrupted after starting the consumer
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        QueryContainer container =
                new QueryContainer(new CountDownLatch(1), future, responseHandlerMock, new HashSet<>(), 1);
        container.startStreamPublisher();
        container.write(TargetQueryResponse.newBuilder().build());
        // verify that the consumer thread has started
        Mockito.verify(responseHandlerMock, timeout(60000).times(1)).handleResponse(Mockito.any());

        // make the consumer thread very slow - onNext() call will not return for a long time
        Mockito.doAnswer((Answer<Void>) invocation -> {
            Thread.sleep(Integer.MAX_VALUE);
            return null;
        }).when(responseHandlerMock).handleResponse(Mockito.any());

        CountDownLatch latch = new CountDownLatch(1);
        // start a new thread mimicking a reader task
        Thread readerTask = new Thread(() -> {
            try {
                latch.countDown();
                while (true) {
                    container.write(TargetQueryResponse.newBuilder().build());
                }
            } catch (Throwable e) {
                e.printStackTrace();
            }
        });
        readerTask.start();
        // make sure that the reader task is started
        latch.await();

        // interrupt the readerTask so that it interrupts the consumer thread.
        readerTask.interrupt();
        Assertions.assertFalse(future.get());
    }

    @Test
    void testCompleteWithSuccess() throws ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(1);
        QueryContainer container = new QueryContainer(latch, future, responseHandlerMock, new HashSet<>(), 1);
        container.startStreamPublisher();
        TargetQueryResponse response = TargetQueryResponse.newBuilder().build();
        container.write(response);
        Mockito.verify(responseHandlerMock, timeout(60000).times(1)).handleResponse(response);
        container.reportReaderTaskComplete(true);
        Assertions.assertEquals(0, latch.getCount());
        Assertions.assertTrue(future.get());
    }

    @Test
    void testCompleteWithFailure() throws ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        // buffer size is set to 1
        QueryContainer container =
                new QueryContainer(new CountDownLatch(1), future, responseHandlerMock, new HashSet<>(), 1);
        container.startStreamPublisher();
        TargetQueryResponse response = TargetQueryResponse.newBuilder().build();
        container.write(response);
        Mockito.verify(responseHandlerMock, timeout(60000).times(1)).handleResponse(response);
        // simulate failure of a reader task
        container.reportReaderTaskComplete(false);
        Assertions.assertFalse(future.get());
    }

    @Test
    void testRunTermination1() throws ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        CountDownLatch latch = new CountDownLatch(2);
        QueryContainer container = new QueryContainer(latch, future, responseHandlerMock, new HashSet<>(), 1);
        container.startStreamPublisher();
        // make sure thread has started
        TargetQueryResponse response = TargetQueryResponse.newBuilder().build();
        container.write(response);
        // make sure that the reader task is started
        Mockito.verify(responseHandlerMock, timeout(60000).atLeastOnce()).handleResponse(response);
        latch.countDown();
        latch.countDown();
        Assertions.assertTrue(future.get());
    }

    @Test
    void testRunTermination2() throws ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        QueryContainer container =
                new QueryContainer(new CountDownLatch(0), future, responseHandlerMock, new HashSet<>(), 32);
        // make sure thread has started
        TargetQueryResponse response = TargetQueryResponse.newBuilder().build();
        container.write(response);
        container.write(response);
        // make sure all messages are published even though the latch has count 0
        container.startStreamPublisher();
        // make sure that the reader task is started
        Mockito.verify(responseHandlerMock, timeout(60000).times(2)).handleResponse(response);
        Assertions.assertTrue(future.get());
    }
}
