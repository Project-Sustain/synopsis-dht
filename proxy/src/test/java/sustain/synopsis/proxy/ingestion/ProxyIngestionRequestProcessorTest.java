package sustain.synopsis.proxy.ingestion;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.dht.Ring;
import sustain.synopsis.dht.store.services.IngestionRequest;
import sustain.synopsis.dht.store.services.IngestionResponse;
import sustain.synopsis.dht.store.services.Strand;
import sustain.synopsis.dht.store.services.TerminateSessionResponse;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ProxyIngestionRequestProcessorTest {

    @Mock
    Ring ringMock;

    @Test
    void testIngestionResponseMergeHelperClass() {
        ProxyIngestionRequestProcessor.IngestionResponseMergeHelper helper =
                new ProxyIngestionRequestProcessor.IngestionResponseMergeHelper();
        IngestionResponse base = helper.getEmptyMessage();
        //  response for a successful operation
        IngestionResponse successResponse =
                IngestionResponse.newBuilder().setDatasetId("dataset1").setSessionId(1001).setMessageId(0)
                                 .setStatus(true).build();
        // response for a failed operation
        IngestionResponse failureResponse =
                IngestionResponse.newBuilder().setDatasetId("dataset1").setSessionId(1001).setMessageId(0)
                                 .setStatus(false).build();

        Assertions.assertEquals(IngestionResponse.getDefaultInstance().toBuilder().setStatus(true).build(), base);

        IngestionResponse result = helper.merge(base, successResponse);
        Assertions.assertEquals("dataset1", result.getDatasetId());
        Assertions.assertEquals(1001, result.getSessionId());
        Assertions.assertEquals(0, result.getMessageId());
        Assertions.assertTrue(result.getStatus());

        result = helper.merge(base, failureResponse);
        Assertions.assertEquals("dataset1", result.getDatasetId());
        Assertions.assertEquals(1001, result.getSessionId());
        Assertions.assertEquals(0, result.getMessageId());
        Assertions.assertFalse(result.getStatus());

        // check the different merging orders
        // success, success -> success
        result = helper.merge(base, successResponse);
        result = helper.merge(result, successResponse);
        Assertions.assertEquals("dataset1", result.getDatasetId());
        Assertions.assertEquals(1001, result.getSessionId());
        Assertions.assertEquals(0, result.getMessageId());
        Assertions.assertTrue(result.getStatus());

        // success, failure -> failure
        result = helper.merge(base, successResponse);
        result = helper.merge(result, failureResponse);
        Assertions.assertEquals("dataset1", result.getDatasetId());
        Assertions.assertEquals(1001, result.getSessionId());
        Assertions.assertEquals(0, result.getMessageId());
        Assertions.assertFalse(result.getStatus());

        // failure, success -> failure
        result = helper.merge(base, failureResponse);
        result = helper.merge(result, successResponse);
        Assertions.assertEquals("dataset1", result.getDatasetId());
        Assertions.assertEquals(1001, result.getSessionId());
        Assertions.assertEquals(0, result.getMessageId());
        Assertions.assertFalse(result.getStatus());

        // failure, failure -> failure
        result = helper.merge(base, failureResponse);
        result = helper.merge(result, failureResponse);
        Assertions.assertEquals("dataset1", result.getDatasetId());
        Assertions.assertEquals(1001, result.getSessionId());
        Assertions.assertEquals(0, result.getMessageId());
        Assertions.assertFalse(result.getStatus());
    }

    @Test
    void testTerminateSessionResponseMergeHelperClass() {
        ProxyIngestionRequestProcessor.TerminateSessionResponseMergeHelper helper =
                new ProxyIngestionRequestProcessor.TerminateSessionResponseMergeHelper();
        TerminateSessionResponse base = helper.getEmptyMessage();
        TerminateSessionResponse successfulResponse = TerminateSessionResponse.newBuilder().setStatus(true).build();
        TerminateSessionResponse failureResponse = TerminateSessionResponse.newBuilder().setStatus(false).build();

        Assertions.assertTrue(base.getStatus()); // required for merging the status

        TerminateSessionResponse mergeResult = helper.merge(base, successfulResponse);
        Assertions.assertTrue(mergeResult.getStatus());

        mergeResult = helper.merge(base, failureResponse);
        Assertions.assertFalse(mergeResult.getStatus());

        mergeResult = helper.merge(base, successfulResponse);
        mergeResult = helper.merge(mergeResult, successfulResponse);
        Assertions.assertTrue(mergeResult.getStatus());

        mergeResult = helper.merge(base, successfulResponse);
        mergeResult = helper.merge(mergeResult, failureResponse);
        Assertions.assertFalse(mergeResult.getStatus());

        mergeResult = helper.merge(base, failureResponse);
        mergeResult = helper.merge(mergeResult, successfulResponse);
        Assertions.assertFalse(mergeResult.getStatus());

        mergeResult = helper.merge(base, failureResponse);
        mergeResult = helper.merge(mergeResult, failureResponse);
        Assertions.assertFalse(mergeResult.getStatus());
    }

    @Test
    void testGetKey() {
        LocalDateTime ts = LocalDateTime.of(2020, 4, 15, 0, 7, 45);
        ProxyIngestionRequestProcessor proxyIngestionRequestProcessor = new ProxyIngestionRequestProcessor();
        Strand strand = Strand.newBuilder().setEntityId("9xji").setFromTs(CommonUtil.localDateTimeToEpoch(ts)).build();
        Assertions.assertEquals("9xji:4", proxyIngestionRequestProcessor.getKey(strand));
    }

    @Test
    void testSplit() {
        MockitoAnnotations.initMocks(this);
        long ts = CommonUtil.localDateTimeToEpoch(LocalDateTime.of(2020, 4, 15, 0, 7, 45));
        IngestionRequest.Builder builder =
                IngestionRequest.newBuilder().setDatasetId("dataset").setSessionId(100).setMessageId(1);
        // partition 1
        builder.addStrand(Strand.newBuilder().setEntityId("9xh").setFromTs(ts).build());
        builder.addStrand(Strand.newBuilder().setEntityId("9xi").setFromTs(ts).build());
        // partition 2
        builder.addStrand(Strand.newBuilder().setEntityId("9xj").setFromTs(ts).build());
        IngestionRequest request = builder.build();

        // partition 1
        Mockito.when(ringMock.lookup("9xh:4")).thenReturn("host1:9000");
        Mockito.when(ringMock.lookup("9xi:4")).thenReturn("host1:9000");
        // partition 2
        Mockito.when(ringMock.lookup("9xj:4")).thenReturn("host2:9000");

        ProxyIngestionRequestProcessor proxyIngestionRequestProcessor = new ProxyIngestionRequestProcessor(ringMock);
        Map<String, IngestionRequest> splits = proxyIngestionRequestProcessor.split(request);
        Assertions.assertEquals(2, splits.size());
        // check the individual ingestion requests
        IngestionRequest split = splits.get("host1:9000");
        Assertions.assertEquals("dataset", split.getDatasetId());
        Assertions.assertEquals(100, split.getSessionId());
        Assertions.assertEquals(1, split.getMessageId());
        Assertions.assertEquals(2, split.getStrandCount());
        Set<String> split1Entities =
                split.getStrandList().stream().map(Strand::getEntityId).collect(Collectors.toSet());
        Set<String> expectedEntities = new HashSet<>(Arrays.asList("9xh", "9xi"));
        Assertions.assertEquals(expectedEntities, split1Entities);

        split = splits.get("host2:9000");
        Assertions.assertEquals("dataset", split.getDatasetId());
        Assertions.assertEquals(100, split.getSessionId());
        Assertions.assertEquals(1, split.getMessageId());
        Assertions.assertEquals(1, split.getStrandCount());
        Assertions.assertEquals("9xj", split.getStrand(0).getEntityId());
    }


    void testGetStub(){

    }
}
