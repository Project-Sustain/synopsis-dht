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
package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.storage.lsmtree.compress.BlockCompressor;
import sustain.synopsis.storage.lsmtree.compress.LZ4BlockCompressor;

import java.io.*;
import java.util.*;

class SSTableWriterTest {

    private static final int SEED = 123;

    @Test
    void testSerialize() throws IOException {
        // serialized size of an entry is 20 Bytes
        Metadata<LSMTestKey> metadata = new Metadata<>();
        byte[] serialized = getSerializedSSTable(metadata, null, null);

        // check the metadata
        Assertions.assertEquals(new LSMTestKey(0), metadata.getMin());
        Assertions.assertEquals(new LSMTestKey(100), metadata.getMax());
        Map<LSMTestKey, Integer> blockIndex = metadata.getBlockIndex();
        Assertions.assertEquals(5, blockIndex.size());

        // check if the data is serialized correctly
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        Random random = new Random(123);
        int blockCount = 0;
        int i = 0;

        boolean hasMoreBlocks = true;
        while (hasMoreBlocks) {
            hasMoreBlocks = dis.readBoolean();
            dis.readBoolean(); // compression status
            byte[] block = new byte[dis.readInt()];
            dis.readFully(block);
            blockCount++;
            DataInputStream blockInputStream = new DataInputStream(new ByteArrayInputStream(block));
            while (blockInputStream.available() > 0) {
                LSMTestKey k = new LSMTestKey();
                LSMTestValue v = new LSMTestValue();
                k.deserialize(blockInputStream);
                v.deserialize(blockInputStream);
                Assertions.assertEquals(new LSMTestKey(i++), k);
                byte[] expectedVal = new byte[12]; // total entry size is 20. (key -4, length encoding - 4, payload -
                // 12)
                random.nextBytes(expectedVal); // payloads are generated in a deterministic ways using the same seed
                Assertions.assertEquals(new LSMTestValue(expectedVal), v);
            }
        }
        Assertions.assertEquals(5, blockCount);

        // check if the index is correctly generated.
        for (Map.Entry<LSMTestKey, Integer> entry : blockIndex.entrySet()) {
            dis = new DataInputStream(new ByteArrayInputStream(serialized));
            dis.skipBytes(entry.getValue());
            dis.readBoolean();
            dis.readBoolean(); // compression status
            byte[] block = new byte[dis.readInt()];
            dis.readFully(block);
            DataInputStream blockInputStream = new DataInputStream(new ByteArrayInputStream(block));
            LSMTestKey testKey = new LSMTestKey();
            testKey.deserialize(blockInputStream);
            Assertions.assertEquals(entry.getKey(), testKey);
        }
    }

    @Test
    void testSerializationWithExactBlockFill() throws IOException {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 10, 20,
                        new Random(123))));
        SSTableWriter<LSMTestKey, LSMTestValue> writer = new SSTableWriter<>(200, Collections.singletonList(mergeIter));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Metadata<LSMTestKey> metadata = new Metadata<>();
        writer.serialize(dos, metadata, null, null);
        dos.flush();
        baos.flush();

        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        boolean haveMoreBlocks = dis.readBoolean();
        boolean isCompressed = dis.readBoolean(); // compression status
        int size = dis.readInt();
        // there should not be other blocks
        Assertions.assertEquals(dis.available(), size);
        Assertions.assertFalse(haveMoreBlocks);
        Assertions.assertFalse(isCompressed);
        Assertions.assertEquals(1, metadata.getBlockIndex().size());
    }

    @Test
    void testSerializationEmptySSTable() throws IOException {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 0, 20,
                        new Random(123))));
        SSTableWriter<LSMTestKey, LSMTestValue> writer = new SSTableWriter<>(200, Collections.singletonList(mergeIter));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        Metadata<LSMTestKey> metadata = new Metadata<>();
        writer.serialize(dos, metadata, null, null);
        dos.flush();
        baos.flush();

        byte[] serialized = baos.toByteArray();
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        boolean haveMoreBlocks = dis.readBoolean();
        boolean isCompressed = dis.readBoolean();
        int size = dis.readInt();
        // there should not be other blocks
        Assertions.assertEquals(0, size);
        Assertions.assertFalse(isCompressed);
        Assertions.assertFalse(haveMoreBlocks);
        Assertions.assertEquals(0, metadata.getBlockIndex().size());
    }

    @Test
    void testSerializationWithCompression() throws IOException {
        // serialized size of an entry is 20 Bytes
        BlockCompressor compressor = new LZ4BlockCompressor();
        Metadata<LSMTestKey> metadata = new Metadata<>();
        byte[] serialized = getSerializedSSTable(metadata, compressor, null);
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);
        Iterator<LSMTestKey> blockIndexIterator = metadata.getBlockIndex().keySet().iterator();

        Random random = new Random(SEED);
        int i = 0;
        while (blockIndexIterator.hasNext()) {
            // checks if the block index is updated properly when the compression is enabled.
            dis.skipBytes(metadata.getBlockIndex().get(blockIndexIterator.next()));
            dis.readBoolean();
            boolean isCompressed = dis.readBoolean();
            Assertions.assertTrue(isCompressed);
            int decompressedLength = dis.readInt();
            int compressedLength = dis.readInt();
            byte[] block = new byte[compressedLength];
            dis.readFully(block);
            byte[] decompressed = compressor.decompress(decompressedLength, block);
            // check if the data is valid
            DataInputStream blockInputStream = new DataInputStream(new ByteArrayInputStream(decompressed));
            while (blockInputStream.available() > 0) {
                LSMTestKey k = new LSMTestKey();
                LSMTestValue v = new LSMTestValue();
                k.deserialize(blockInputStream);
                v.deserialize(blockInputStream);
                Assertions.assertEquals(new LSMTestKey(i++), k);
                byte[] expectedVal = new byte[12]; // total entry size is 20. (key -4, length encoding - 4, payload -
                // 12)
                random.nextBytes(expectedVal); // payloads are generated in a deterministic ways using the same seed
                Assertions.assertEquals(new LSMTestValue(expectedVal), v);
            }
            dis.reset(); // offsets are cumulative. so it is required to reset the input stream counter
        }
    }

    @Test
    void testSerializationWithChecksum() throws ChecksumGenerator.ChecksumError, IOException {
        Metadata<LSMTestKey> metadata = new Metadata<>();
        ChecksumGenerator checksumGenerator = new ChecksumGenerator();
        byte[] serialized = getSerializedSSTable(metadata, null, checksumGenerator);
        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);

        Map<LSMTestKey, Integer> blockIndex = metadata.getBlockIndex();
        Map<LSMTestKey, byte[]> checksums = metadata.getChecksums();

        Assertions.assertEquals(blockIndex.size(), checksums.size());

        for (LSMTestKey key : blockIndex.keySet()) {
            int offset = blockIndex.get(key);
            dis.skipBytes(offset);
            // read the block
            dis.readBoolean();
            dis.readBoolean();
            byte[] block = new byte[dis.readInt()];
            dis.readFully(block);
            byte[] checksum = checksumGenerator.calculateChecksum(block);
            Assertions.assertTrue(Arrays.equals(checksum, checksums.get(key)));
            dis.reset();
        }
    }

    private byte[] getSerializedSSTable(Metadata<LSMTestKey> metadata, BlockCompressor compressor,
                                        ChecksumGenerator checksumGenerator) throws IOException {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIter =
                new SortedMergeIterator<>(Collections.singletonList(TestUtil.getIterator(0, 1, 101, 20,
                        new Random(SEED))));
        SSTableWriter<LSMTestKey, LSMTestValue> ssTableWriter = new SSTableWriter<>(500,
                Collections.singletonList(mergeIter));
        // total data size = 20 * 101 = 2020
        // block count = 5
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(byteArrayOutputStream);
        ssTableWriter.serialize(dos, metadata, compressor, checksumGenerator);
        dos.flush();
        byteArrayOutputStream.flush();
        return byteArrayOutputStream.toByteArray();
    }
}
