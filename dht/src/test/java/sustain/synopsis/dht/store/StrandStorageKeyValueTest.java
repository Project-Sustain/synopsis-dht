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
package sustain.synopsis.dht.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class StrandStorageKeyValueTest {
    private long from = 1391216400000L;
    private long to = from + 100;

    public static Strand createStrand(String geohash, long ts, long to, long sessionId, double... features) {
        Path path = new Path(features.length);
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
    }

    public static Strand createStrand(String geohash, long ts, long to, double... features) {
        Path path = new Path(features.length);
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
    }

    public static byte[] serializeStrand(Strand strand) throws IOException {
        return CommonUtil.strandToProtoBuff(strand).toByteArray();
    }

    @Test
    void testStrandStorageKeyCompare() {
        StrandStorageKey key = new StrandStorageKey(from, to);
        StrandStorageKey greaterKey = new StrandStorageKey(from + 10, to);
        StrandStorageKey lowerKey = new StrandStorageKey(from - 10, from);
        StrandStorageKey equalKey = new StrandStorageKey(from, to);
        Assertions.assertTrue(key.compareTo(greaterKey) < 0);
        Assertions.assertTrue(key.compareTo(lowerKey) > 0);
        Assertions.assertEquals(0, key.compareTo(equalKey));
        Assertions.assertEquals(key, equalKey);
        Assertions.assertEquals(key.hashCode(), equalKey.hashCode());
    }

    @Test
    void testStrandStorageKeySerialization() throws IOException {
        StrandStorageKey key = new StrandStorageKey(from, to);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] serialized;
        try {
            key.serialize(dos);
            dos.flush();
            baos.flush();
            serialized = baos.toByteArray();
        } finally {
            dos.close();
            baos.close();
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);
        StrandStorageKey deserializedKey = new StrandStorageKey();
        try {
            deserializedKey.deserialize(dis);
            Assertions.assertEquals(key, deserializedKey);
        } finally {
            bais.close();
            dis.close();
        }
    }

    @Test
    void testStrandStorageValueMerge() throws IOException {
        Strand strand1 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        ProtoBuffSerializedStrand protoBuffSerializedStrand1 = CommonUtil.strandToProtoBuff(strand1);

        StrandStorageValue value1 = new StrandStorageValue(protoBuffSerializedStrand1.toByteArray());
        Assertions.assertEquals(Collections.singletonList(strand1), value1.getStrands());
        Assertions.assertEquals(Collections.singletonList(protoBuffSerializedStrand1), value1.getProtoBuffSerializedStrands());

        // test merging
        Strand strand2 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        ProtoBuffSerializedStrand protoBuffSerializedStrand2 = CommonUtil.strandToProtoBuff(strand2);
        StrandStorageValue value2 = new StrandStorageValue(protoBuffSerializedStrand2.toByteArray());
        value1.merge(value2);

        List<ProtoBuffSerializedStrand> returnedProtoBuffSerializedStrands = value1.getProtoBuffSerializedStrands();
        Assertions.assertEquals(2, returnedProtoBuffSerializedStrands.size());
        Assertions.assertTrue(returnedProtoBuffSerializedStrands.contains(protoBuffSerializedStrand1));
        Assertions.assertTrue(returnedProtoBuffSerializedStrands.contains(protoBuffSerializedStrand2));

        List<Strand> returnedStrands = value1.getStrands();
        Assertions.assertEquals(2, returnedStrands.size());
        Assertions.assertTrue(returnedStrands.contains(strand1));
        Assertions.assertTrue(returnedStrands.contains(strand2));
    }

    @Test
    void testStrandStorageValueSerialization() throws IOException {
        Strand strand1 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        Strand strand2 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        StrandStorageValue val = new StrandStorageValue(serializeStrand(strand1));
        val.merge(new StrandStorageValue(serializeStrand(strand2)));

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] serializedData;
        try {
            val.serialize(dos);
            baos.flush();
            dos.flush();
            serializedData = baos.toByteArray();
        } finally {
            baos.close();
            dos.close();
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
        DataInputStream dis = new DataInputStream(bais);
        StrandStorageValue deserialized = new StrandStorageValue();
        try {
            deserialized.deserialize(dis);
            List<Strand> returnedStrands = deserialized.getStrands();
            Assertions.assertEquals(2, returnedStrands.size());
            Assertions.assertTrue(returnedStrands.contains(strand1));
            Assertions.assertTrue(returnedStrands.contains(strand2));
        } finally {
            bais.close();
            dis.close();
        }
    }

    @Test
    void testStrandStorageKeyToString() {
        StrandStorageKey key = new StrandStorageKey(from, to);
        Assertions.assertEquals(from + "_" + to, key.toString());
    }

    @Test
    void testStrandStorageKeyGetters() {
        StrandStorageKey key = new StrandStorageKey(10, 20);
        Assertions.assertEquals(10, key.getStartTS());
        Assertions.assertEquals(20, key.getEndTS());
    }
}
