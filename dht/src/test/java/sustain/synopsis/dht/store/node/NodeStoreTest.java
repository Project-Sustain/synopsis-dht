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
package sustain.synopsis.dht.store.node;

import org.apache.commons.collections4.Trie;
import org.apache.commons.collections4.trie.PatriciaTrie;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.common.Strand;
import sustain.synopsis.dht.Context;
import sustain.synopsis.dht.NodeConfiguration;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.services.ingestion.WriterPool;
import sustain.synopsis.dht.services.query.QueryException;
import sustain.synopsis.dht.store.*;
import sustain.synopsis.dht.store.entity.EntityStore;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.dht.store.services.TargetQueryRequest;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.createStrand;
import static sustain.synopsis.dht.store.StrandStorageKeyValueTest.serializeStrand;

public class NodeStoreTest {

    @TempDir
    File metadataStoreDir;

    @TempDir
    File storageDir;

    @Mock
    Logger loggerMock;

    @Mock
    SessionValidator sessionValidatorMock;

    @Mock
    DiskManager diskManagerMock;

    @Mock
    EntityStore entityStoreMock1;

    @Mock
    EntityStore entityStoreMock2;

    @Test
    void testFreshStart() throws StorageException, IOException {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
        MockitoAnnotations.initMocks(this);
        Mockito.when(loggerMock.iterator()).thenReturn(new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public byte[] next() {
                return null;
            }
        });
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(true);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());

        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        nodeStore.init();
        assertTrue(nodeStore.entityStoreMap.isEmpty());
        assertTrue(nodeStore.validatedSessions.isEmpty());
        nodeStore.store("dataset_1", "entity_1", 1000L, new StrandStorageKey(1391216400000L, 1391216400100L),
                        new StrandStorageValue(
                                serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertTrue(nodeStore.entityStoreMap.containsKey("dataset_1"));
        assertTrue(nodeStore.entityStoreMap.get("dataset_1").containsKey("entity_1"));
        assertTrue(nodeStore.validatedSessions.containsKey(1000L));

        String entityCommitLogPath =
                metadataStoreDir.getAbsolutePath() + File.separator + "dataset_1_entity_1_metadata" + ".slog";
        Mockito.verify(sessionValidatorMock, Mockito.times(1)).validate("dataset_1", 1000L);
        Mockito.verify(loggerMock, Mockito.times(1))
               .append(new CreateEntityStoreActivity("dataset_1", "entity_1").serialize());
        // we can verify if there is a log written into the entity store log to make sure the store request is made to
        // to the entity store
        // log appenders are initialized
        File entityCommitLog = new File(entityCommitLogPath);
        assertTrue(entityCommitLog.exists());

        // add more data for the same entity store
        Strand strand = createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0);
        nodeStore.store("dataset_1", "entity_1", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(strand.getFromTimeStamp(), strand.getToTimestamp()),
                        new StrandStorageValue(serializeStrand(strand)));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertEquals(1, nodeStore.entityStoreMap.get("dataset_1").size());

        // add data to a different entity store
        strand = createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0);
        nodeStore.store("dataset_1", "entity_2", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(strand.getFromTimeStamp(), strand.getToTimestamp()),
                        new StrandStorageValue(serializeStrand(strand)));
        assertEquals(1, nodeStore.entityStoreMap.size());
        assertEquals(2, nodeStore.entityStoreMap.get("dataset_1").size());
        assertTrue(nodeStore.entityStoreMap.get("dataset_1").containsKey("entity_2"));

        // add data to a different dataset
        strand = createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0);
        nodeStore.store("dataset_2", "entity_2", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(strand.getFromTimeStamp(), strand.getToTimestamp()),
                        new StrandStorageValue(serializeStrand(strand)));
        assertEquals(2, nodeStore.entityStoreMap.size());
        assertTrue(nodeStore.entityStoreMap.containsKey("dataset_2"));

        // test end_session
        WriterPool writerPool = new WriterPool(2);
        List<CompletableFuture<Boolean>> endSessionFutures = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(2, endSessionFutures.size());
        CompletableFuture.allOf(endSessionFutures.toArray(new CompletableFuture[0])).thenApply(
                future -> endSessionFutures.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertTrue);

        List<CompletableFuture<Boolean>> futures = nodeStore.endSession("dataset_2", 1000L, writerPool);
        Assertions.assertEquals(1, futures.size());
        futures.get(0).thenAccept(Assertions::assertTrue);
    }

    @Test
    void testRestart() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        Mockito.when(sessionValidatorMock.validate("dataset_2", 1001L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "alice", 7896L));
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        nodeConfiguration.setMetadataStoreDir(metadataStoreDir.getAbsolutePath());
        nodeConfiguration.setMemTableSize(1024);
        nodeConfiguration.setBlockSize(256);
        nodeConfiguration.setRootJournalLoc(metadataStoreDir.getAbsolutePath());
        Map<String, Long> storageDirs = new HashMap<>();
        storageDirs.put(storageDir.getAbsolutePath(), 10240L);
        nodeConfiguration.setStorageDirs(storageDirs);
        nodeConfiguration.setStorageAllocationPolicy("round-robin");
        Context.getInstance().initialize(nodeConfiguration);

        NodeStore nodeStore = new NodeStore();
        nodeStore.init();
        nodeStore.store("dataset_1", "entity_1", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(1391216400000L, 1391216400100L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        nodeStore.store("dataset_1", "entity_2", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.store("dataset_2", "entity_2", new IngestionSession("bob", 123456L, 1001L),
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        Assertions.assertTrue(nodeStore.endEntityStoreSession("dataset_1", "entity_1", "bob", 1000L, 123456L));
        Assertions.assertTrue(nodeStore.endEntityStoreSession("dataset_1", "entity_2", "bob", 1000L, 123456L));
        Assertions.assertTrue(nodeStore.endEntityStoreSession("dataset_2", "entity_2", "bob", 1001L, 123456L));


        // simulate a restarted node store by starting a NodeStore by pointing to the same commit log
        NodeStore nodeStoreRestarted = new NodeStore();
        nodeStoreRestarted.init();
        assertEquals(2, nodeStoreRestarted.entityStoreMap.size());
        assertEquals(2, nodeStoreRestarted.entityStoreMap.get("dataset_1").size());
        assertTrue(nodeStoreRestarted.entityStoreMap.get("dataset_1").containsKey("entity_1"));
        assertTrue(nodeStoreRestarted.entityStoreMap.get("dataset_1").containsKey("entity_2"));
        assertEquals(1, nodeStoreRestarted.entityStoreMap.get("dataset_2").size());
        assertTrue(nodeStoreRestarted.entityStoreMap.get("dataset_2").containsKey("entity_2"));
    }

    @Test
    void testEndSession() throws StorageException, IOException {
        MockitoAnnotations.initMocks(this);
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
        Mockito.when(loggerMock.iterator()).thenReturn(new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public byte[] next() {
                return null;
            }
        });
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(true);

        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        nodeStore.init();
        WriterPool writerPool = new WriterPool(2);

        // valid session, but no such dataset
        nodeStore.validatedSessions.clear();
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        List<CompletableFuture<Boolean>> futures1 = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(1, futures1.size());
        CompletableFuture.allOf(futures1.toArray(new CompletableFuture[0])).thenApply(
                future -> futures1.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertTrue);

        // invalid session
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(false, "bob", 12345L));
        List<CompletableFuture<Boolean>> futures2 = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(1, futures2.size());
        CompletableFuture.allOf(futures2.toArray(new CompletableFuture[0])).thenApply(
                future -> futures2.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertFalse);

        // add mock entity stores
        // reset the session validator validate the session as valid
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        IngestionSession session = new IngestionSession("bob", 12345L, 1000L);
        Mockito.when(entityStoreMock1.endSession(session)).thenReturn(true);
        Mockito.when(entityStoreMock2.endSession(session)).thenReturn(true);
        Trie<String, EntityStore> entityStores = new PatriciaTrie<>();
        entityStores.put("entity_1", entityStoreMock1);
        entityStores.put("entity_2", entityStoreMock2);
        nodeStore.entityStoreMap.put("dataset_1", entityStores);
        List<CompletableFuture<Boolean>> futures3 = nodeStore.endSession("dataset_1", 1000L, writerPool);
        Assertions.assertEquals(2, futures3.size());
        CompletableFuture.allOf(futures2.toArray(new CompletableFuture[0])).thenApply(
                future -> futures3.stream().map(CompletableFuture::join).reduce(true, (b1, b2) -> b1 && b2))
                         .thenAccept(Assertions::assertTrue);
        Mockito.verify(entityStoreMock1, Mockito.times(1)).endSession(session);
        Mockito.verify(entityStoreMock2, Mockito.times(1)).endSession(session);
    }

    @Test
    void testIngestionSession() {
        IngestionSession session1 = new IngestionSession("bob", 1234L, 1000L);
        IngestionSession session2 = new IngestionSession("bob", 12344L, 1000L);
        IngestionSession session3 = new IngestionSession("bob", 1234L, 1001L);

        assertEquals(session1, session2); // same session id
        assertNotEquals(session1, session3); // different session ids
    }

    @Test
    void testDiskManagerInitFail() {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
        MockitoAnnotations.initMocks(this);
        Mockito.when(loggerMock.iterator()).thenReturn(new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public byte[] next() {
                return null;
            }
        });
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(false);
        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        Assertions.assertThrows(StorageException.class, nodeStore::init);
    }

    @Test
    void testGetMatchingEntityStoresForCorrectDataset() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        Predicate predicate = Predicate.newBuilder().setStringValue("8qr").build();
        TargetQueryRequest req =
                TargetQueryRequest.newBuilder().setDataset("dataset_2").addSpatialScope(predicate).build();
        Set<EntityStore> matchingStores = nodeStore.getMatchingEntityStores(req);
        Assertions.assertEquals(1, matchingStores.size());
        Assertions.assertEquals("8qr", matchingStores.iterator().next().getEntityId());
    }

    @Test
    void testGetMatchingEntityStoresForInvalidDataset() throws StorageException, IOException {
        NodeStore nodeStore = prepareTestNodeStore();
        Predicate predicate = Predicate.newBuilder().setStringValue("8qr").build();
        TargetQueryRequest finalReq =
                TargetQueryRequest.newBuilder().setDataset("non_existing_dataset").addSpatialScope(predicate).build();
        Assertions.assertEquals(0, nodeStore.getMatchingEntityStores(finalReq).size());
    }

    @Test
    void testGetMatchingEntityStoresForZeroSpatialScope() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        TargetQueryRequest emptyReq = TargetQueryRequest.newBuilder().setDataset("dataset_1").build();
        Assertions.assertTrue(nodeStore.getMatchingEntityStores(emptyReq).isEmpty());
    }

    @Test
    void testGetMatchingEntityStoresForInvalidSpatialScope() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        Predicate predicate = Predicate.newBuilder().setStringValue("9yyi").build();
        TargetQueryRequest req =
                TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertTrue(nodeStore.getMatchingEntityStores(req).isEmpty());
    }

    @Test
    void testGetMatchingEntityStoresForPrefixMatching() throws StorageException, IOException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        // use a single character - should match with all entity stores
        Predicate predicate = Predicate.newBuilder().setStringValue("9").build();
        TargetQueryRequest req =
                TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5", "9x")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));

        // three characters
        predicate = Predicate.newBuilder().setStringValue("9xi").build();
        req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));

        // exact match
        predicate = Predicate.newBuilder().setStringValue("9xi5").build();
        req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate).build();
        Assertions.assertEquals(new HashSet<>(Collections.singletonList("9xi5")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));
    }

    @Test
    void testGetMatchingEntityStoresForPrefixMatchingWithMultipleSpatialScopes()
            throws IOException, StorageException, QueryException {
        NodeStore nodeStore = prepareTestNodeStore();
        // non-overlapping scopes
        Predicate predicate1 = Predicate.newBuilder().setStringValue("9xi").build();
        Predicate predicate2 = Predicate.newBuilder().setStringValue("8x").build();
        TargetQueryRequest req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate1).
                addSpatialScope(predicate2).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5", "8x")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));

        // overallping scopes
        predicate2 = Predicate.newBuilder().setStringValue("9xi5").build();
        req = TargetQueryRequest.newBuilder().setDataset("dataset_1").addSpatialScope(predicate1).
                addSpatialScope(predicate2).build();
        Assertions.assertEquals(new HashSet<>(Arrays.asList("9xi", "9xi5")),
                                nodeStore.getMatchingEntityStores(req).stream().map(EntityStore::getEntityId)
                                         .collect(Collectors.toSet()));
    }

    private NodeStore prepareTestNodeStore() throws StorageException, IOException {
        NodeConfiguration nodeConfiguration = new NodeConfiguration();
        Context.getInstance().initialize(nodeConfiguration);
        MockitoAnnotations.initMocks(this);
        Mockito.when(loggerMock.iterator()).thenReturn(new Iterator<byte[]>() {
            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public byte[] next() {
                return null;
            }
        });
        Mockito.when(sessionValidatorMock.validate("dataset_1", 1000L))
               .thenReturn(new SessionValidator.SessionValidationResponse(true, "bob", 12345L));
        Mockito.when(diskManagerMock.init(nodeConfiguration)).thenReturn(true);
        Mockito.when(diskManagerMock.allocate(Mockito.anyLong())).thenReturn(storageDir.getAbsolutePath());

        NodeStore nodeStore =
                new NodeStore(sessionValidatorMock, loggerMock, 1024, 200, metadataStoreDir.getAbsolutePath(),
                              diskManagerMock);
        nodeStore.init();
        // dataset_1
        nodeStore.store("dataset_1", "9xi", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(1391216400000L, 1391216400100L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        nodeStore.store("dataset_1", "9xi5", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.store("dataset_1", "9x", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.store("dataset_1", "8x", new IngestionSession("bob", 123456L, 1000L),
                        new StrandStorageKey(1391216400100L, 1391216400200L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400100L, 1391216400200L, 1.0, 2.0))));
        nodeStore.endEntityStoreSession("dataset_1", "9xi", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "9xi5", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "9x", "bob", 1000L, 123456L);
        nodeStore.endEntityStoreSession("dataset_1", "8x", "bob", 1000L, 123456L);

        // dataset_2
        nodeStore.store("dataset_2", "8qr", new IngestionSession("bob", 123456L, 1001L),
                        new StrandStorageKey(1391216400000L, 1391216400100L), new StrandStorageValue(
                        serializeStrand(createStrand("9xj", 1391216400000L, 1391216400100L, 1.0, 2.0))));
        nodeStore.endEntityStoreSession("dataset_2", "8qr", "bob", 1001L, 123456L);
        return nodeStore;
    }
}
