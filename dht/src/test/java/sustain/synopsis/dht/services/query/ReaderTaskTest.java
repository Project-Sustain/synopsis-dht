package sustain.synopsis.dht.services.query;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.StrandStorageKeyValueTest;
import sustain.synopsis.dht.store.StrandStorageValue;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.dht.store.services.TargetQueryRequest;
import sustain.synopsis.dht.store.services.TargetQueryResponse;
import sustain.synopsis.storage.lsmtree.Metadata;
import sustain.synopsis.storage.lsmtree.SSTableReader;
import sustain.synopsis.storage.lsmtree.TableIterator;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ReaderTaskTest {
    @Mock
    QueryContainer containerMock;

    @Mock
    EntityStore entityStoreMock1;

    @Mock
    EntityStore entityStoreMock2;

    @TempDir
    File tempDir;

    @Test
    void testTargetQueryResponseWrapper() {
        MockitoAnnotations.initMocks(this);
        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock);
        int batchSizeLimit = 1024;
        ReaderTask.TargetQueryResponseWrapper wrapper = task.new TargetQueryResponseWrapper(batchSizeLimit);
        // without any data
        wrapper.close();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());

        Mockito.reset(containerMock);
        // with one strand - one strand is 62 bytes
        ProtoBuffSerializedStrand strand = CommonUtil
                .strandToProtoBuff(StrandStorageKeyValueTest.createStrand("9xj", 1000, 2000, 100.0, 122.0, 513.4));
        wrapper.addProtoBuffSerializedStrand(strand);
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());

        Mockito.reset(containerMock);
        wrapper.close();
        Mockito.verify(containerMock, Mockito.times(1))
               .write(TargetQueryResponse.newBuilder().addStrands(strand).build());

        // check if the responses are published when the message size reaches the limit
        long serializedSize = strand.getSerializedSize(); // 62 bytes
        int batchSize = (int) Math.ceil(batchSizeLimit / (serializedSize * 1.0));
        for (int batchId = 0; batchId < 3; batchId++) {
            TargetQueryResponse.Builder builder = TargetQueryResponse.newBuilder();
            Mockito.reset(containerMock);
            for (int i = 0; i < batchSize; i++) {
                long fromTS = 1000 * batchId + i;
                strand = CommonUtil.strandToProtoBuff(
                        StrandStorageKeyValueTest.createStrand("9xj", fromTS, fromTS + 100, 100.0, 122.0, 513.4));
                wrapper.addProtoBuffSerializedStrand(strand);
                builder.addStrands(strand);
            }
            Mockito.verify(containerMock, Mockito.times(1)).write(builder.build());
        }
        Mockito.reset(containerMock);
        wrapper.close();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());
    }

//    @Test
//    void testSendStrandsAsBatchesConversionToMatchingStrand() throws IOException {
//        MockitoAnnotations.initMocks(this);
//        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("test_entity");
//        // use a small batch size to make sure that the response gets published to the container immediately
//        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock, 1);
//
//        ByteString serializedStrand =
//                StrandStorageKeyValueTest.createStrand("9xj", 1000, 2000, 100.0, 122.0, 513.4).serializeAsProtoBuff();
//        ProtoBuffSerializedStrand strand = ProtoBuffSerializedStrand.newBuilder().mergeFrom(serializedStrand).build();
//        TargetQueryResponse targetQueryResponse = TargetQueryResponse.newBuilder().addStrands(strand).build();
//        task.appendToResponse(Collections.singletonList(new TableIterator.TableEntry<>(new StrandStorageKey(1000, 2000),
//                                                                                       new StrandStorageValue(
//                                                                                               serializedStrand
//                                                                                                       .toByteArray()))), 1);
//        Mockito.verify(containerMock, Mockito.times(1)).write(targetQueryResponse);
//    }

//    @Test
//    void testStrandStorageValueWithMultipleStrands() throws IOException {
//        MockitoAnnotations.initMocks(this);
//        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("test_entity");
//        // use a small batch size to make sure that the response gets published to the container immediately
//        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock, 1);
//
//        ProtoBuffSerializedStrand strand1 = CommonUtil
//                .strandToProtoBuff(StrandStorageKeyValueTest.createStrand("9xj", 1000, 2000, 100.0, 122.0, 513.4));
//        ProtoBuffSerializedStrand strand2 = CommonUtil
//                .strandToProtoBuff(StrandStorageKeyValueTest.createStrand("9xj", 1000, 2000, 100.0, 123.0, 523.4));
//        StrandStorageValue strandStorageValue = new StrandStorageValue(strand1.toByteArray(), strand2.toByteArray());
//        List<TableIterator.TableEntry<StrandStorageKey, StrandStorageValue>> matchingBlockData = Collections
//                .singletonList(new TableIterator.TableEntry<>(new StrandStorageKey(1000, 2000), strandStorageValue));
//        task.appendToResponse(matchingBlockData, 1);
//        Mockito.verify(containerMock, Mockito.times(1))
//               .write(TargetQueryResponse.newBuilder().addStrands(strand1).build());
//        Mockito.verify(containerMock, Mockito.times(1))
//               .write(TargetQueryResponse.newBuilder().addStrands(strand2).build());
//    }

    @Test
    void testSendStrandsAsBatchesMultipleBatches() throws IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("test_entity");
        int batchSizeInBytes = 1024 * 1024;
        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock, batchSizeInBytes);
        byte[] serializedStrand =
                StrandStorageKeyValueTest.createStrand("9xj", 1000, 2000, 100.0, 122.0, 513.4).serializeAsProtoBuff()
                                         .toByteArray();
        StrandStorageValue strandStorageValue = new StrandStorageValue(serializedStrand);
        // send enough data for two batches
        int messageCount = (int) Math.ceil(batchSizeInBytes * 2.0 / serializedStrand.length);
        List<TableIterator.TableEntry<StrandStorageKey, StrandStorageValue>> matchingStrands =
                IntStream.range(0, messageCount).mapToObj(
                        i -> new TableIterator.TableEntry<>(new StrandStorageKey(i, i + 1), strandStorageValue))
                         .collect(Collectors.toList());
        task.appendToResponse(matchingStrands, 1);
        Mockito.verify(containerMock, Mockito.times(2)).write(Mockito.any());
    }

    @Test
    void testReadBlock() throws IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("test_entity");

        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(10, 2, metadata);

        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock, 1024 * 1024);
        SSTableReader<StrandStorageKey, StrandStorageValue> reader = new SSTableReader<>(metadata,
                                                                                         StrandStorageKey.class,
                                                                                         StrandStorageValue.class);
        List<TableIterator.TableEntry<StrandStorageKey, StrandStorageValue>> strands =
                task.readBlock(reader, new StrandStorageKey(0, 1), Collections.singletonList(new Interval(0, 20)));
        Assertions.assertEquals(10, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(9, 10), strands.get(9).getKey());

        reader = new SSTableReader<>(metadata, StrandStorageKey.class, StrandStorageValue.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1), Collections.singletonList(new Interval(0, 5)));
        Assertions.assertEquals(5, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(4, 5), strands.get(4).getKey());

        reader = new SSTableReader<>(metadata, StrandStorageKey.class, StrandStorageValue.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1), Collections.singletonList(new Interval(10, 5)));
        Assertions.assertEquals(0, strands.size());

        // multiple intervals
        reader = new SSTableReader<>(metadata, StrandStorageKey.class, StrandStorageValue.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1),
                                 Arrays.asList(new Interval(0, 5), new Interval(10, 5)));
        Assertions.assertEquals(5, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(4, 5), strands.get(4).getKey());

        reader = new SSTableReader<>(metadata, StrandStorageKey.class, StrandStorageValue.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1),
                                 Arrays.asList(new Interval(0, 5), new Interval(3, 5)));
        Assertions.assertEquals(5, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(4, 5), strands.get(4).getKey());
    }

    @Test
    void testReadSSTable() throws IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("test_entity");

        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(1, 5, metadata); // each block will have a single strand
        // we use a smallest possible batchSize threshold to ensure that every strand gets published right away
        // from to the container
        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock, 1);

        // all 5 blocks should be included
        MatchedSSTable matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(0, 50));
        task.readSSTable(matchedSSTable);
        Mockito.verify(containerMock, Mockito.times(5)).write(Mockito.any());

        // 1 matching block
        Mockito.reset(containerMock);
        matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(0, 10));
        task.readSSTable(matchedSSTable);
        Mockito.verify(containerMock, Mockito.times(1)).write(Mockito.any());

        // no matching blocks
        Mockito.reset(containerMock);
        matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(50, 100));
        task.readSSTable(matchedSSTable);
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());

        // multiple intervals
        Mockito.reset(containerMock);
        matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(0, 10));
        matchedSSTable.addMatchedInterval(new Interval(10, 30));
        matchedSSTable.addMatchedInterval(new Interval(50, 100));
        task.readSSTable(matchedSSTable);
        Mockito.verify(containerMock, Mockito.times(3)).write(Mockito.any());
    }

    @Test
    void testRunWithOneEntityStore() throws QueryException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("test_entity");
        Mockito.when(containerMock.getNextTask()).thenReturn(entityStoreMock1).thenReturn(null);
        Predicate temporalPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(0)
                         .build();
        Predicate spatialPredicate = Predicate.newBuilder().setStringValue("test_entity").build();
        Expression exp = Expression.newBuilder().setPredicate1(temporalPredicate).build();
        TargetQueryRequest targetQueryRequest =
                TargetQueryRequest.newBuilder().setTemporalScope(exp).addSpatialScope(spatialPredicate).build();

        // no matching results
        Mockito.when(entityStoreMock1.temporalQuery(exp)).thenReturn(new ArrayList<>());
        ReaderTask task = new ReaderTask(targetQueryRequest, containerMock, 1);
        task.run();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).reportReaderTaskComplete(true);

        // two matching blocks per entity store
        Mockito.reset(containerMock);
        Mockito.when(containerMock.getNextTask()).thenReturn(entityStoreMock1).thenReturn(null);
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(1, 2, metadata);
        MatchedSSTable matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(0, 21));
        Mockito.when(entityStoreMock1.temporalQuery(exp)).thenReturn(Collections.singletonList(matchedSSTable));
        task.run();
        Mockito.verify(containerMock, Mockito.times(2)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).reportReaderTaskComplete(true);

        // error during reading
        Mockito.reset(containerMock);
        Mockito.when(containerMock.getNextTask()).thenReturn(entityStoreMock1).thenReturn(null);
        File f = new File(tempDir + File.separator + "non_existing_file");
        f.createNewFile(); // empty file
        metadata.setPath(f.getAbsolutePath());
        Mockito.reset(containerMock);
        task.run();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).reportReaderTaskComplete(true);
    }

    @Test
    void testRunWithMultipleEntityStores() throws QueryException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock1.getEntityId()).thenReturn("store_1");
        Mockito.when(entityStoreMock2.getEntityId()).thenReturn("store_2");
        Mockito.when(containerMock.getNextTask()).thenReturn(entityStoreMock1).thenReturn(entityStoreMock2)
               .thenReturn(null);
        Expression exp = Expression.newBuilder().build();
        TargetQueryRequest targetQueryRequest = TargetQueryRequest.newBuilder().setTemporalScope(exp).build();
        Mockito.when(entityStoreMock1.temporalQuery(Mockito.any())).thenReturn(new ArrayList<>());

        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(1, 2, metadata);
        MatchedSSTable matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(0, 21));
        Mockito.when(entityStoreMock2.temporalQuery(Mockito.any()))
               .thenReturn(Collections.singletonList(matchedSSTable));

        ReaderTask task = new ReaderTask(targetQueryRequest, containerMock, 1024 * 1024);
        task.run();
        Mockito.verify(containerMock, Mockito.times(3)).getNextTask();
        Mockito.verify(entityStoreMock1, Mockito.times(1)).temporalQuery(exp);
        Mockito.verify(entityStoreMock2, Mockito.times(1)).temporalQuery(exp);
        // make sure the wrapper is closed
        Mockito.verify(containerMock, Mockito.times(1)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).reportReaderTaskComplete(true);
    }

    @Test
    void testRunWithReadError() throws QueryException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(containerMock.getNextTask()).thenReturn(entityStoreMock1).thenReturn(null);
        Mockito.when(entityStoreMock1.temporalQuery(Mockito.any())).thenThrow(new QueryException("Invalid Query"));
        ReaderTask task = new ReaderTask(TargetQueryRequest.newBuilder().build(), containerMock, 1024 * 1024);
        task.run();
        // test fail fast approach during query handling
        Mockito.verify(containerMock, Mockito.times(1)).getNextTask();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).reportReaderTaskComplete(false);
    }

    private void prepareSSTable(int strandsPerBlock, int blockCount, Metadata<StrandStorageKey> metadata)
            throws IOException {
        // Maximum - 10 strands per block
        File f = new File(tempDir + File.separator + "temp.file");
        metadata.setPath(f.getAbsolutePath());

        StrandStorageValue strandStorageValue = new StrandStorageValue(
                StrandStorageKeyValueTest.createStrand("9xj", 1000, 2000, 100.0, 122.0, 513.4).serializeAsProtoBuff()
                                         .toByteArray());
        BlockCompressor compressor = new LZ4BlockCompressor();
        FileOutputStream fos = new FileOutputStream(f);
        DataOutputStream dos = new DataOutputStream(fos);
        for (int blockId = 0; blockId < blockCount; blockId++) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream blockDataOS = new DataOutputStream(baos);
            for (int i = 0; i < strandsPerBlock; i++) {
                StrandStorageKey key = new StrandStorageKey(blockId * 10 + i, (blockId * 10 + i) + 1);
                if (i == 0) {
                    // update the index
                    metadata.addBlockIndex(key, dos.size());
                }
                key.serialize(blockDataOS);
                strandStorageValue.serialize(blockDataOS);
            }
            blockDataOS.flush();
            baos.flush();
            byte[] block = baos.toByteArray();
            byte[] compressedData = compressor.compress(block);
            dos.writeBoolean(blockId == 1); // two blocks
            dos.writeBoolean(true); // compressed
            dos.writeInt(block.length);
            dos.writeInt(compressedData.length);
            dos.write(compressedData);
            blockDataOS.close();
            baos.close();
        }
        dos.flush();
        fos.flush();
        dos.close();
        fos.close();
    }
}
