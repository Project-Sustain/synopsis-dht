package sustain.synopsis.dht.store.query;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.node.NodeStore;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class QuerySchedulerTest {
    @Mock
    ExecutorService executorService;

    @Mock
    NodeStore nodeStore;

    @Mock
    EntityStore entityStoreMock1;

    @Mock
    EntityStore entityStoreMock2;

    @Mock
    StreamObserver<TargetQueryResponse> responseObserverMock;

    @Test
    void testSchedulerWithNoMatchingEntityStores() throws QueryException, ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        TargetQueryRequest request = TargetQueryRequest.newBuilder().build();
        Mockito.when(nodeStore.getMatchingEntityStores(request)).thenReturn(Collections.emptySet());
        QueryScheduler scheduler = new QueryScheduler(nodeStore, executorService);
        CompletableFuture<Boolean> future = scheduler.schedule(request, responseObserverMock, 1);
        // there should not be any reader tasks created
        Mockito.verify(executorService, Mockito.never()).submit(Mockito.any(ReaderTask.class));
        Assertions.assertTrue(future.get());
    }

    @Test
    void testScheduleReaderCount() throws QueryException, ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        TargetQueryRequest request = TargetQueryRequest.newBuilder().build();
        Mockito.when(nodeStore.getMatchingEntityStores(request))
               .thenReturn(new HashSet<>(Arrays.asList(entityStoreMock1, entityStoreMock2)));
        QueryScheduler scheduler = new QueryScheduler(nodeStore, executorService);
        scheduler.schedule(request, responseObserverMock, 2);
        // there should not be any reader tasks created
        Mockito.verify(executorService, Mockito.times(2)).submit(Mockito.any(ReaderTask.class));
    }
}
