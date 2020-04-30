package sustain.synopsis.dht.services.query;

import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.services.TargetQueryResponse;

public class GrpcQueryResponseHandlerTest {
    @Mock
    private StreamObserver<TargetQueryResponse> responseStreamObserverMock;

    @Test
    void testHandleResponse(){
        MockitoAnnotations.initMocks(this);
        GrpcQueryResponseHandler responseHandler = new GrpcQueryResponseHandler(responseStreamObserverMock);
        TargetQueryResponse resp = TargetQueryResponse.newBuilder().build();
        responseHandler.handleResponse(resp);
        Mockito.verify(responseStreamObserverMock, Mockito.times(1)).onNext(resp);
    }

    @Test
    void testHandleError(){
        MockitoAnnotations.initMocks(this);
        GrpcQueryResponseHandler responseHandler = new GrpcQueryResponseHandler(responseStreamObserverMock);
        Throwable t = new Throwable();
        responseHandler.handleError(t);
        Mockito.verify(responseStreamObserverMock, Mockito.times(1)).onError(t);
    }
}
