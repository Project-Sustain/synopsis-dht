package sustain.synopsis.dht.store.query;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.MatchingStrand;
import sustain.synopsis.dht.store.services.TargetQueryResponse;
import sustain.synopsis.storage.lsmtree.TableIterator;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReaderTaskTest {
    @Mock
    QueryContainer containerMock;

    @Mock
    EntityStore entityStoreMock;

    @Test
    void testSendStrandsAsBatchesConversionToMatchingStrand() {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock.getEntityId()).thenReturn("test_entity");
        ReaderTask task = new ReaderTask(entityStoreMock, null, containerMock, 1024 * 1024);

        MatchingStrand matchingStrand = MatchingStrand.newBuilder().setFromTS(1).setToTS(2).setSpatialScope(
                "test_entity").setStrand(ByteString.copyFrom(new byte[100])).build();
        TargetQueryResponse targetQueryResponse = TargetQueryResponse.newBuilder().addStrands(matchingStrand).build();
        task.sendStrandsAsBatches(Collections.singletonList(new TableIterator.TableEntry<>(new StrandStorageKey(1, 2)
                , new byte[100])));
        Mockito.verify(containerMock, Mockito.times(1)).write(targetQueryResponse);
    }

    @Test
    void testSendStrandsAsBatchesMultipleBatches(){
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock.getEntityId()).thenReturn("test_entity");
        ReaderTask task = new ReaderTask(entityStoreMock, null, containerMock, 1024 * 1024);
        // with 1 MB batch size, there will be 2 strands per response msg.
        List<TableIterator.TableEntry<StrandStorageKey, byte[]>> matchingStrands =
                IntStream.range(0, 3).mapToObj(i -> new TableIterator.TableEntry<>(new StrandStorageKey(i, i + 1),
                        new byte[512 * 1024])).collect(Collectors.toList());
        task.sendStrandsAsBatches(matchingStrands);
        // We will have 1 full response, and 1 partially filled response
        Mockito.verify(containerMock, Mockito.times(2)).write(Mockito.any());
    }
}
