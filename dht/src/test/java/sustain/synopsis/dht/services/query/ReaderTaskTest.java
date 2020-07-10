/*
 *
 * Software in the Sustain Ecosystem are Released Under Terms of Apache Software License 
 *
 * This research has been supported by funding from the US National Science Foundation's CSSI program through awards 1931363, 1931324, 1931335, and 1931283. The project is a joint effort involving Colorado State University, Arizona State University, the University of California-Irvine, and the University of Maryland - Baltimore County. All redistributions of the software must also include this information. 
 *
 * TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
 *
 *
 * 1. Definitions.
 *
 * "License" shall mean the terms and conditions for use, reproduction, and distribution as defined by Sections 1 through 9 of this document.
 *
 * "Licensor" shall mean the copyright owner or entity authorized by the copyright owner that is granting the License.
 *
 * "Legal Entity" shall mean the union of the acting entity and all other entities that control, are controlled by, or are under common control with that entity. For the purposes of this definition, "control" means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.
 *
 * "You" (or "Your") shall mean an individual or Legal Entity exercising permissions granted by this License.
 *
 * "Source" form shall mean the preferred form for making modifications, including but not limited to software source code, documentation source, and configuration files.
 *
 * "Object" form shall mean any form resulting from mechanical transformation or translation of a Source form, including but not limited to compiled object code, generated documentation, and conversions to other media types.
 *
 * "Work" shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work (an example is provided in the Appendix below).
 *
 * "Derivative Works" shall mean any work, whether in Source or Object form, that is based on (or derived from) the Work and for which the editorial revisions, annotations, elaborations, or other modifications represent, as a whole, an original work of authorship. For the purposes of this License, Derivative Works shall not include works that remain separable from, or merely link (or bind by name) to the interfaces of, the Work and Derivative Works thereof.
 *
 * "Contribution" shall mean any work of authorship, including the original version of the Work and any modifications or additions to that Work or Derivative Works thereof, that is intentionally submitted to Licensor for inclusion in the Work by the copyright owner or by an individual or Legal Entity authorized to submit on behalf of the copyright owner. For the purposes of this definition, "submitted" means any form of electronic, verbal, or written communication sent to the Licensor or its representatives, including but not limited to communication on electronic mailing lists, source code control systems, and issue tracking systems that are managed by, or on behalf of, the Licensor for the purpose of discussing and improving the Work, but excluding communication that is conspicuously marked or otherwise designated in writing by the copyright owner as "Not a Contribution."
 *
 * "Contributor" shall mean Licensor and any individual or Legal Entity on behalf of whom a Contribution has been received by Licensor and subsequently incorporated within the Work.
 *
 * 2. Grant of Copyright License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare Derivative Works of, publicly display, publicly perform, sublicense, and distribute the Work and such Derivative Works in Source or Object form.
 *
 * 3. Grant of Patent License. Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such license applies only to those patent claims licensable by such Contributor that are necessarily infringed by their Contribution(s) alone or by combination of their Contribution(s) with the Work to which such Contribution(s) was submitted. If You institute patent litigation against any entity (including a cross-claim or counterclaim in a lawsuit) alleging that the Work or a Contribution incorporated within the Work constitutes direct or contributory patent infringement, then any patent licenses granted to You under this License for that Work shall terminate as of the date such litigation is filed.
 *
 * 4. Redistribution. You may reproduce and distribute copies of the Work or Derivative Works thereof in any medium, with or without modifications, and in Source or Object form, provided that You meet the following conditions:
 *
 * You must give any other recipients of the Work or Derivative Works a copy of this License; and
 * You must cause any modified files to carry prominent notices stating that You changed the files; and
 * You must retain, in the Source form of any Derivative Works that You distribute, all copyright, patent, trademark, and attribution notices from the Source form of the Work, excluding those notices that do not pertain to any part of the Derivative Works; and
 * If the Work includes a "NOTICE" text file as part of its distribution, then any Derivative Works that You distribute must include a readable copy of the attribution notices contained within such NOTICE file, excluding those notices that do not pertain to any part of the Derivative Works, in at least one of the following places: within a NOTICE text file distributed as part of the Derivative Works; within the Source form or documentation, if provided along with the Derivative Works; or, within a display generated by the Derivative Works, if and wherever such third-party notices normally appear. The contents of the NOTICE file are for informational purposes only and do not modify the License. You may add Your own attribution notices within Derivative Works that You distribute, alongside or as an addendum to the NOTICE text from the Work, provided that such additional attribution notices cannot be construed as modifying the License. 
 *
 * You may add Your own copyright statement to Your modifications and may provide additional or different license terms and conditions for use, reproduction, or distribution of Your modifications, or for any such Derivative Works as a whole, provided Your use, reproduction, and distribution of the Work otherwise complies with the conditions stated in this License.
 * 5. Submission of Contributions. Unless You explicitly state otherwise, any Contribution intentionally submitted for inclusion in the Work by You to the Licensor shall be under the terms and conditions of this License, without any additional terms or conditions. Notwithstanding the above, nothing herein shall supersede or modify the terms of any separate license agreement you may have executed with Licensor regarding such Contributions.
 *
 * 6. Trademarks. This License does not grant permission to use the trade names, trademarks, service marks, or product names of the Licensor, except as required for reasonable and customary use in describing the origin of the Work and reproducing the content of the NOTICE file.
 *
 * 7. Disclaimer of Warranty. Unless required by applicable law or agreed to in writing, Licensor provides the Work (and each Contributor provides its Contributions) on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of permissions under this License.
 *
 * 8. Limitation of Liability. In no event and under no legal theory, whether in tort (including negligence), contract, or otherwise, unless required by applicable law (such as deliberate and grossly negligent acts) or agreed to in writing, shall any Contributor be liable to You for damages, including any direct, indirect, special, incidental, or consequential damages of any character arising as a result of this License or out of the use or inability to use the Work (including but not limited to damages for loss of goodwill, work stoppage, computer failure or malfunction, or any and all other commercial damages or losses), even if such Contributor has been advised of the possibility of such damages.
 *
 * 9. Accepting Warranty or Additional Liability. While redistributing the Work or Derivative Works thereof, You may choose to offer, and charge a fee for, acceptance of support, warranty, indemnity, or other liability obligations and/or rights consistent with this License. However, in accepting such obligations, You may act only on Your own behalf and on Your sole responsibility, not on behalf of any other Contributor, and only if You agree to indemnify, defend, and hold each Contributor harmless for any liability incurred by, or claims asserted against, such Contributor by reason of your accepting any such warranty or additional liability. 
 *
 * END OF TERMS AND CONDITIONS */
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
