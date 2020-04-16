package sustain.synopsis.dht.services.query;

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

public class DHTQueryProcessorTest {
    @Mock
    ExecutorService executorServiceMock;

    @Mock
    NodeStore nodeStore;

    @Mock
    EntityStore entityStoreMock1;

    @Mock
    EntityStore entityStoreMock2;

    @Mock
    EntityStore entityStoreMock3;

    @Mock
    StreamObserver<TargetQueryResponse> responseObserverMock;

    @Test
    void testSchedulerWithNoMatchingEntityStores() throws ExecutionException, InterruptedException {
        MockitoAnnotations.initMocks(this);
        TargetQueryRequest request = TargetQueryRequest.newBuilder().build();
        Mockito.when(nodeStore.getMatchingEntityStores(request)).thenReturn(Collections.emptySet());
        DHTQueryProcessor scheduler = new DHTQueryProcessor(nodeStore, executorServiceMock, 2);
        CompletableFuture<Boolean> future = scheduler.process(request, responseObserverMock);
        // there should not be any reader tasks created
        Mockito.verify(executorServiceMock, Mockito.never()).submit(Mockito.any(ReaderTask.class));
        Assertions.assertTrue(future.get());
    }

    @Test
    void testScheduleReaderCount() {
        MockitoAnnotations.initMocks(this);
        TargetQueryRequest request = TargetQueryRequest.newBuilder().build();
        // when the number of matching entities is less than the number of cores
        Mockito.when(nodeStore.getMatchingEntityStores(request))
               .thenReturn(new HashSet<>(Collections.singletonList(entityStoreMock1)));
        DHTQueryProcessor scheduler = new DHTQueryProcessor(nodeStore, executorServiceMock, 2);
        scheduler.process(request, responseObserverMock);
        Mockito.verify(executorServiceMock, Mockito.times(1)).submit(Mockito.any(ReaderTask.class));

        Mockito.when(nodeStore.getMatchingEntityStores(request))
               .thenReturn(new HashSet<>(Arrays.asList(entityStoreMock1, entityStoreMock2, entityStoreMock3)));
        Mockito.reset(executorServiceMock);
        scheduler.process(request, responseObserverMock);
        Mockito.verify(executorServiceMock, Mockito.times(2)).submit(Mockito.any(ReaderTask.class));
    }
}
