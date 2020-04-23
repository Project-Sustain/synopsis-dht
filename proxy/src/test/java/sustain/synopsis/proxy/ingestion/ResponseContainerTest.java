package sustain.synopsis.proxy.ingestion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.services.IngestionResponse;

public class ResponseContainerTest {

    @Mock
    ProxyIngestionRequestProcessor.IngestionResponseMergeHelper helperMock;

    @Test
    void testMergeInvocation() {
        MockitoAnnotations.initMocks(this);
        int responseCount = 10;
        int parallelismLevel = 5;
        ResponseContainer<IngestionResponse> container =
                new ResponseContainer<>(responseCount, helperMock, parallelismLevel);
        Mockito.verify(helperMock, Mockito.times(parallelismLevel)).getEmptyMessage();

        for (int i = 0; i < responseCount - 1; i++) {
            container.handleResponse(IngestionResponse.newBuilder().setDatasetId("dataset").setSessionId(1).setMessageId(1000 + i)
                                                      .setStatus(true).build());
            Mockito.verify(helperMock, Mockito.times(1)).merge(Mockito.any(), Mockito.any());
            Mockito.reset(helperMock);
        }
        // all responses are received
        Assertions.assertTrue(container.handleResponse(IngestionResponse.newBuilder().setDatasetId("dataset").setSessionId(1)
                                                                        .setMessageId(1000 + responseCount).setStatus(true)
                                                                        .build()));
        Mockito.verify(helperMock, Mockito.times(1)).merge(Mockito.any(), Mockito.any());
        Mockito.reset(helperMock);

        container.getMergedResponse();
        Mockito.verify(helperMock, Mockito.times(parallelismLevel - 1)).merge(Mockito.any(), Mockito.any());
    }

    @Test
    void testResponseContainer() {
        int responseCount = 10;
        ResponseContainer<IngestionResponse> container =
                new ResponseContainer<>(responseCount, new ResponseMergeHelper<IngestionResponse>() {
                    @Override
                    public IngestionResponse getEmptyMessage() {
                        return IngestionResponse.newBuilder().setStatus(true).setDatasetId("dataset").setSessionId(1)
                                                .setMessageId(0).build();
                    }

                    @Override
                    public IngestionResponse merge(IngestionResponse base, IngestionResponse newResponse) {
                        // we add up the message ids to show if the final merge is correctly invoked
                        return base.toBuilder().setMessageId(base.getMessageId() + newResponse.getMessageId()).build();
                    }
                }, 2);


        for (int i = 0; i < responseCount; i++) {
            container.handleResponse(IngestionResponse.newBuilder().setDatasetId("dataset").setSessionId(1).setMessageId(1)
                                                      .setStatus(true).build());
        }

        IngestionResponse resp = container.getMergedResponse();
        Assertions.assertEquals("dataset", resp.getDatasetId());
        Assertions.assertEquals(1, resp.getSessionId());
        Assertions.assertTrue(resp.getStatus());
        Assertions.assertEquals(responseCount, resp.getMessageId());
    }
}
