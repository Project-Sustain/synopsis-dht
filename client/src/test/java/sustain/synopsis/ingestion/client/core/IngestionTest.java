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
package sustain.synopsis.ingestion.client.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.publishing.StrandPublisher;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.*;

class IngestionTest {

    @Mock
    private StrandRegistry registryMock;

    @Mock
    private StrandPublisher publisherMock;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void testRecordToStrandConversion() {
        Map<String, Quantizer> quantizers = new HashMap<>();
        quantizers.put("feature_1", new Quantizer(new Feature(0.0f), new Feature(10.0f), new Feature(20.0f)));
        quantizers.put("feature_2", new Quantizer(new Feature(100.0f), new Feature(200.0f), new Feature(1000.0f)));

        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(Duration.ofHours(1));
        StrandConversionTask ingestionTask = new StrandConversionTask(null, null, quantizers, Duration.ofHours(1),
                new CountDownLatch(1), new CountDownLatch(1));
        Record record = new Record();
        record.setGeohash("9xj");
        long ts = CommonUtil.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 23));
        record.setTimestamp(ts);
        record.addFeatureValue("feature_1", 3.0f);
        record.addFeatureValue("feature_2", 140.0f);
        Strand strand = ingestionTask.convertToStrand(record);

        //  check the spatio-temporal attributes
        long[] boundaries = temporalQuantizer.getTemporalBoundaries(ts);
        assertEquals(boundaries[0], strand.getFromTimeStamp());
        assertEquals(boundaries[1], strand.getToTimestamp());
        assertEquals("9xj", strand.getGeohash());

        // check the path
        Path path = strand.getPath();
        assertEquals("feature_1", path.get(0).getLabel().getName());
        assertEquals(0.0, path.get(0).getLabel().getDouble()); // discretized value

        assertEquals("feature_2", path.get(1).getLabel().getName());
        assertEquals(100.0, path.get(1).getLabel().getDouble()); // discretized value
        assertNotNull(path.get(1).getData()); // check the data container

        // add a new feature that does not have a quantizer
        record.addFeatureValue("feature_3", 3.01f);
        assertThrows(AssertionError.class, () -> {
            ingestionTask.convertToStrand(record);
        });
    }

    @Test
    void testIngestionTaskExecution() {
        Map<String, Quantizer> quantizers = new HashMap<>();
        quantizers.put("feature_1", new Quantizer(new Feature(0.0f), new Feature(10.0f), new Feature(20.0f)));
        quantizers.put("feature_2", new Quantizer(new Feature(100.0f), new Feature(200.0f), new Feature(1000.0f)));

        ArrayBlockingQueue<Record> input = new ArrayBlockingQueue<>(5);
        StrandConversionTask ingestionTask = new StrandConversionTask(registryMock, input, quantizers, Duration.ofHours(1),
                new CountDownLatch(1), new CountDownLatch(1));
        Thread t = new Thread(ingestionTask);
        t.start();

        Record r1 = new Record();
        r1.setGeohash("9xj");
        r1.setTimestamp(CommonUtil.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 23)));
        r1.addFeatureValue("feature_1", 20.0f);
        r1.addFeatureValue("feature_2", 200.0f);

        Record r2 = new Record();
        r2.setGeohash("9xj");
        r2.setTimestamp(CommonUtil.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 24)));
        r2.addFeatureValue("feature_1", 20.0f);
        r2.addFeatureValue("feature_2", 200.0f);

        input.add(r1);
        input.add(r2);
        // allow a few seconds for the thread to pick up the data - there is a 5 second wait if the queue is empty
        // setting a longer timeout to avoid false positive test failures due to a slower process
        Mockito.verify(registryMock, Mockito.timeout(20 * 1000).times(2)).add(Mockito.any(Strand.class));

        ingestionTask.terminate();
        Mockito.verify(registryMock, Mockito.timeout(20 * 1000).times(1)).terminateSession();
    }

    @Test
    void testStrandRegistryAdd() {
        LocalDateTime from = LocalDateTime.of(2019, 2, 12, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2019, 2, 12, 1, 1, 0);
        Strand strand1 = createStrand(new Path(), "9xj", CommonUtil.localDateTimeToEpoch(from),
                                      CommonUtil.localDateTimeToEpoch(to), 1.0, 2.0);
        StrandRegistry registry = new StrandRegistry((messageId, strands) -> {

        });
        assertEquals(1, registry.add(strand1));

        // add a different strand
        Strand strand2 = createStrand(new Path(), "9xj", CommonUtil.localDateTimeToEpoch(from),
                                      CommonUtil.localDateTimeToEpoch(to), 1.1, 2.0);
        assertEquals(2, registry.add(strand2));

        // add a strand for merging
        Strand similarStrand = createStrand(new Path(), "9xj", CommonUtil.localDateTimeToEpoch(from),
                                            CommonUtil.localDateTimeToEpoch(to), 1.0, 2.0);
        assertEquals(2, registry.add(similarStrand));
    }

    @Test
    void testStrandPublishing() {
        LocalDateTime from = LocalDateTime.of(2019, 2, 12, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2019, 2, 12, 1, 1, 0);
        Strand strand1 = createStrand(new Path(), "9xk", CommonUtil.localDateTimeToEpoch(from),
                                      CommonUtil.localDateTimeToEpoch(to), 1.0, 2.0);
        // same geohash and temporal bounds, different feature values.
        Strand strand2 = createStrand(new Path(), "9xi", CommonUtil.localDateTimeToEpoch(from),
                                      CommonUtil.localDateTimeToEpoch(to), 1.1, 2.0);
        // different geohash
        Strand strand3 = createStrand(new Path(), "9xj", CommonUtil.localDateTimeToEpoch(from),
                                      CommonUtil.localDateTimeToEpoch(to), 1.0, 2.0);
        // increments the timestamp for the prefix 9xj. Should trigger publishing strand 1 and strand 2
        Strand strand4 = createStrand(new Path(), "9xh",
                                      CommonUtil.localDateTimeToEpoch(from.plusMinutes(1)),
                                      CommonUtil.localDateTimeToEpoch(to.plusMinutes(1)), 1.0, 2.0);

        StrandRegistry registry = new StrandRegistry(publisherMock);
        registry.add(strand1);
        registry.add(strand2);
        registry.add(strand3);
        registry.add(strand4);


        // check the session termination
        long total = registry.terminateSession();
        assertEquals(4, total);

        // strands1 and strands2 should get published
        // - strand4 increments the timestamp
        List<Strand> expectedOutput = new ArrayList<>();
        expectedOutput.add(strand1);
        expectedOutput.add(strand2);
        expectedOutput.add(strand3);
        expectedOutput.add(strand4);
        Mockito.verify(publisherMock, Mockito.timeout(1)).publish(0, expectedOutput);
    }

    private Strand createStrand(Path path, String geohash, long ts, long to, double... features) {
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
    }
}
