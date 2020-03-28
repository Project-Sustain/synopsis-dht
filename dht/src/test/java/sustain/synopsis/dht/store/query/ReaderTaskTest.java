package sustain.synopsis.dht.store.query;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.*;
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
    EntityStore entityStoreMock;

    @TempDir
    File tempDir;

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
    void testSendStrandsAsBatchesMultipleBatches() {
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

    @Test
    void testReadBlock() throws IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock.getEntityId()).thenReturn("test_entity");

        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(10, 2, metadata);

        ReaderTask task = new ReaderTask(entityStoreMock, null, containerMock, 1024 * 1024);
        SSTableReader<StrandStorageKey> reader = new SSTableReader<>(metadata, StrandStorageKey.class);
        List<TableIterator.TableEntry<StrandStorageKey, byte[]>> strands = task.readBlock(reader,
                new StrandStorageKey(0, 1), Collections.singletonList(new Interval(0, 20)));
        Assertions.assertEquals(10, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(9, 10), strands.get(9).getKey());

        reader = new SSTableReader<>(metadata, StrandStorageKey.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1), Collections.singletonList(new Interval(0, 5)));
        Assertions.assertEquals(5, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(4, 5), strands.get(4).getKey());

        reader = new SSTableReader<>(metadata, StrandStorageKey.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1), Collections.singletonList(new Interval(10, 5)));
        Assertions.assertEquals(0, strands.size());

        // multiple intervals
        reader = new SSTableReader<>(metadata, StrandStorageKey.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1), Arrays.asList(new Interval(0, 5),
                new Interval(10, 5)));
        Assertions.assertEquals(5, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(4, 5), strands.get(4).getKey());

        reader = new SSTableReader<>(metadata, StrandStorageKey.class);
        strands = task.readBlock(reader, new StrandStorageKey(0, 1), Arrays.asList(new Interval(0, 5), new Interval(3
                , 5)));
        Assertions.assertEquals(5, strands.size());
        Assertions.assertEquals(new StrandStorageKey(0, 1), strands.get(0).getKey());
        Assertions.assertEquals(new StrandStorageKey(4, 5), strands.get(4).getKey());
    }

    @Test
    void testReadSSTable() throws IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock.getEntityId()).thenReturn("test_entity");

        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(1, 5, metadata); // each block will have a single strand
        ReaderTask task = new ReaderTask(entityStoreMock, null, containerMock, 1024 * 1024);

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
    void testRun() throws QueryException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(entityStoreMock.getEntityId()).thenReturn("test_entity");
        Predicate temporalPredicate =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(0).build();
        Predicate spatialPredicate = Predicate.newBuilder().setStringValue("test_entity").build();
        Expression exp = Expression.newBuilder().setPredicate1(temporalPredicate).build();
        TargetQueryRequest targetQueryRequest =
                TargetQueryRequest.newBuilder().setTemporalScope(exp).addSpatialScope(spatialPredicate).build();

        // no matching results
        Mockito.when(entityStoreMock.temporalQuery(exp)).thenReturn(new ArrayList<>());
        ReaderTask task = new ReaderTask(entityStoreMock, targetQueryRequest, containerMock, 1024 * 1024);
        task.run();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).complete();

        // two matching blocks
        Mockito.reset(containerMock);
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        prepareSSTable(1, 2, metadata);
        MatchedSSTable matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(0, 21));
        Mockito.when(entityStoreMock.temporalQuery(exp)).thenReturn(Collections.singletonList(matchedSSTable));
        task.run();
        Mockito.verify(containerMock, Mockito.times(2)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).complete();

        // error during reading
        File f = new File(tempDir + File.separator + "non_existing_file");
        f.createNewFile(); // empty file
        metadata.setPath(f.getAbsolutePath());
        Mockito.reset(containerMock);
        task.run();
        Mockito.verify(containerMock, Mockito.times(0)).write(Mockito.any());
        Mockito.verify(containerMock, Mockito.times(1)).complete();

    }


    private void prepareSSTable(int strandsPerBlock, int blockCount, Metadata<StrandStorageKey> metadata) throws IOException {
        // Maximum - 10 strands per block
        File f = new File(tempDir + File.separator + "temp.file");
        metadata.setPath(f.getAbsolutePath());

        byte[] payload = new byte[100];
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
                blockDataOS.writeInt(payload.length);
                blockDataOS.write(payload);
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