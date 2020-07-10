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
package sustain.synopsis.dht.store.entity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.services.query.Interval;
import sustain.synopsis.dht.services.query.MatchedSSTable;
import sustain.synopsis.dht.services.query.QueryException;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.serializeStrand;

public class EntityStoreTest {
    @TempDir
    File storageDir;

    @TempDir
    File metadataDir;

    @Mock
    DiskManager diskManagerMock;

    @Mock
    EntityStoreJournal entityStoreJournalMock;

    @Test
    void testSerializedFilePath() throws StorageException, IOException {
        EntityStore store =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 1024, 50, diskManagerMock);
        store.init();
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        String returned = store.getSSTableOutputPath(key1, key2, storageDir.getAbsolutePath(), 0);
        String expected = storageDir.getAbsolutePath() + File.separator + "dataset1_9xj_" + key1.toString() + "_" + key2
                .toString() + "_0.sd";
        assertEquals(expected, returned);
    }

    @Test
    void testToSSTable() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        EntityStore entityStore =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 1024, 50, diskManagerMock);
        entityStore.init();
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        entityStore.store(session, key1, value1);
        entityStore.store(session, key2, value2);
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        entityStore.toSSTable(session, diskManagerMock, metadata);

        File serializedSSTable = new File(
                storageDir.getAbsolutePath() + File.separator + "dataset1_9xj" + "_" + key1.toString() + "_" + key2
                        .toString() + "_0.sd");
        assertTrue(serializedSSTable.exists());
        assertTrue(serializedSSTable.isFile());
        assertTrue(serializedSSTable.length() > 0);
        assertEquals(serializedSSTable.getAbsolutePath(), metadata.getPath());
    }

    @Test
    void testStoreStrand() throws StorageException, IOException {     // testing the entire flow
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        Mockito.when(entityStoreJournalMock.init()).thenReturn(true);
        Mockito.when(entityStoreJournalMock.getSequenceId()).thenReturn(0);
        Mockito.when(entityStoreJournalMock.getMetadata()).thenReturn(new ArrayList<>());
        EntityStore entityStore = new EntityStore("dataset_1", "9xj", entityStoreJournalMock, 80, 20, diskManagerMock);
        entityStore.init();
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).init();

        // start a new session
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).startSession(session);

        assertEquals(1, entityStore.activeSessions.size());
        assertEquals(1, entityStore.activeMetadata.size());

        // size of key and value used here - 187 bytes
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key1, value1); // this should fill up the memTable
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key2, value2);
        // storing 2 strands should fill out the memTable
        Mockito.verify(diskManagerMock, Mockito.times(1)).allocate(Mockito.anyLong());
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).addSerializedSSTable(Mockito.any(), Mockito.any());
        assertEquals(0, entityStore.queryableMetadata.size());
        Metadata<StrandStorageKey> metadata = entityStore.activeMetadata.get(session).get(0);
        assertEquals(key1, metadata.getMin());
        assertEquals(key2, metadata.getMax());
        assertEquals(entityStore.getSSTableOutputPath(key1, key2, storageDir.getAbsolutePath(), 0), metadata.getPath());

        // add more data
        StrandStorageKey key3 = new StrandStorageKey(1391216400200L, 1391216400300L);
        StrandStorageValue value3 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key3, value3);

        // end session
        Assertions.assertTrue(entityStore.endSession(session));
        // at this point, there should be two calls to the disk_manager#allcate method
        Mockito.verify(diskManagerMock, Mockito.times(2)).allocate(Mockito.anyLong());
        Mockito.verify(entityStoreJournalMock, Mockito.times(2)).addSerializedSSTable(Mockito.any(), Mockito.any());
        Mockito.verify(entityStoreJournalMock, Mockito.times(1)).endSession(session);
        assertEquals(2, entityStore.queryableMetadata.size());
        assertEquals(0, entityStore.activeMetadata.size());
        assertEquals(0, entityStore.activeSessions.size());

        // try to an non-existing session
        Assertions.assertTrue(entityStore.endSession(new IngestionSession("alice", System.currentTimeMillis(), 100)));
    }

    @Test
    void testNodeRestart() throws IOException, StorageException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        EntityStore entityStore =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 80, 20, diskManagerMock);
        entityStore.init();
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);
        StrandStorageKey key1 = new StrandStorageKey(1391216400000L, 1391216400100L);
        StrandStorageValue value1 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key1, value1); // this should fill up the memTable
        StrandStorageKey key2 = new StrandStorageKey(1391216400100L, 1391216400200L);
        StrandStorageValue value2 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key2, value2);
        StrandStorageKey key3 = new StrandStorageKey(1391216400200L, 1391216400300L);
        StrandStorageValue value3 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0)));
        entityStore.store(session, key3, value3);
        // end session
        Assertions.assertTrue(entityStore.endSession(session));

        // Simulate a node restart
        EntityStore restartedEntityStore =
                new EntityStore("dataset1", "9xj", metadataDir.getAbsolutePath(), 80, 20, diskManagerMock);
        restartedEntityStore.init();
        // there were two SSTables written before. So the sequence ID should start from 2.
        assertEquals(2, restartedEntityStore.sequenceId.get());
        assertEquals(2, restartedEntityStore.queryableMetadata.size());

        // write some more data
        session = new IngestionSession("bob", System.currentTimeMillis(), 1);
        StrandStorageKey key4 = new StrandStorageKey(1391216400300L, 1391216400400L);
        StrandStorageValue value4 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400300L, 1391216400400L, 1.0, 2.0)));
        restartedEntityStore.store(session, key4, value4); // this should fill up the memTable
        StrandStorageKey key5 = new StrandStorageKey(1391216400400L, 1391216400500L);
        StrandStorageValue value5 =
                new StrandStorageValue(serializeStrand(createStrand("9xj", 1391216400400L, 1391216400500L, 1.0, 2.0)));
        restartedEntityStore.store(session, key5, value5); // this should fill up the memTable

        assertEquals(3, restartedEntityStore.sequenceId.get());
        assertEquals(2, restartedEntityStore.queryableMetadata.size());
        assertEquals(1, restartedEntityStore.activeSessions.size());
        assertEquals(1, restartedEntityStore.activeMetadata.size());
        Assertions.assertTrue(restartedEntityStore.endSession(session));
        assertEquals(3, restartedEntityStore.queryableMetadata.size());
    }

    @Test
    void testQuery() throws StorageException, IOException, QueryException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        Mockito.when(entityStoreJournalMock.init()).thenReturn(true);
        Mockito.when(entityStoreJournalMock.getSequenceId()).thenReturn(0);
        Mockito.when(entityStoreJournalMock.getMetadata()).thenReturn(new ArrayList<>());
        EntityStore entityStore = new EntityStore("dataset1", "9xj", entityStoreJournalMock, 80, 20, diskManagerMock);
        entityStore.init();

        // start a new session
        IngestionSession session = new IngestionSession("bob", System.currentTimeMillis(), 0);
        entityStore.startSession(session);

        // there are no queryable sessions at the moment
        Expression exp = Expression.newBuilder().setPredicate1(
                Predicate.newBuilder().setIntegerValue(1750).setComparisonOp(Predicate.ComparisonOperator.LESS_THAN)
                         .build()).setCombineOp(Expression.CombineOperator.OR).setPredicate2(
                Predicate.newBuilder().setIntegerValue(1800).setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN)
                         .build()).build();
        List<MatchedSSTable> results = entityStore.temporalQuery(exp);
        Assertions.assertTrue(results.isEmpty());

        // size of key and value used here - (52 + 16) bytes
        // storing 2 strands should fill out the memTable
        StrandStorageKey key1 = new StrandStorageKey(1000, 1500);
        StrandStorageValue value1 = new StrandStorageValue(serializeStrand(createStrand("9xj", 1000, 1500, 1.0, 2.0)));
        entityStore.store(session, key1, value1);
        StrandStorageKey key2 = new StrandStorageKey(1500, 2000);
        StrandStorageValue value2 = new StrandStorageValue(serializeStrand(createStrand("9xj", 1500, 2000, 1.0, 2.0)));
        entityStore.store(session, key2, value2);
        // add more data
        StrandStorageKey key3 = new StrandStorageKey(2000, 2500);
        StrandStorageValue value3 = new StrandStorageValue(serializeStrand(createStrand("9xj", 2000, 2500, 1.0, 2.0)));
        entityStore.store(session, key3, value3);
        // end session
        Assertions.assertTrue(entityStore.endSession(session));
        Assertions.assertEquals(2, entityStore.queryableMetadata.size());

        results = entityStore.temporalQuery(exp);
        Assertions.assertEquals(2, results.size());
        // sort the results by the strand timestamps, so that we can check individual values
        results.sort(Comparator.comparing(o -> o.getMetadata().getMin()));
        Assertions.assertEquals(2, results.get(0).getMatchedIntervals().size());
        List<Interval> intervals = results.get(0).getMatchedIntervals();
        intervals.sort(Comparator.comparingLong(Interval::getFrom));
        Assertions.assertEquals(Arrays.asList(new Interval(1000, 1750), new Interval(1801, 2500)), intervals);

        Assertions.assertEquals(1, results.get(1).getMatchedIntervals().size());
        Assertions.assertEquals(Collections.singletonList(new Interval(1801, 2500)),
                                results.get(1).getMatchedIntervals());
    }
}

