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
package sustain.synopsis.samples.client.noaa;

import sustain.synopsis.common.Strand;
import sustain.synopsis.ingestion.client.core.*;
import sustain.synopsis.ingestion.client.publishing.DHTStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.SimpleStrandPublisher;
import sustain.synopsis.ingestion.client.publishing.StrandPublisher;
import sustain.synopsis.metadata.GetMetadataResponse;
import sustain.synopsis.metadata.ProtoBuffSerializedBinConfiguration;
import sustain.synopsis.metadata.ProtoBuffSerializedDatasetMetadata;
import sustain.synopsis.metadata.ProtoBuffSerializedSessionMetadata;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

public class NoaaClient {

    public static final int GEOHASH_LENGTH = 5;
    public static final Duration TEMPORAL_BRACKET_LENGTH = Duration.ofHours(6);

    public static final GetMetadataResponse NOAA_EXAMPLE_METADATA_RESPONSE = GetMetadataResponse.newBuilder()
            .addDatasetMetadata(
                    ProtoBuffSerializedDatasetMetadata.newBuilder()
                            .setDatasetId("noaa-2014")
                            .addSessionMetadata(
                                    ProtoBuffSerializedSessionMetadata.newBuilder()
                                            .setSessionId(1)
                                            .addBinConfiguration(
                                            ProtoBuffSerializedBinConfiguration.newBuilder()
                                    .setFeatureName("precipitable_water_entire_atmosphere")
                                    .addValues((float) -8.34404193162917)
                                    .addValues((float)-1.2860326673303453)
                                    .addValues((float)5.771976596968479)
                                    .addValues((float)12.829985861267302)
                                    .addValues((float)19.88799512556613)
                                    .addValues((float)26.946004389864946)
                                    .addValues((float)34.00401365416377)
                                    .addValues((float)41.062022918462596)
                                    .addValues((float)48.12003218276142)
                                    .addValues((float)55.178041447060245)
                                    .addValues((float)62.23605071135906)
                                    .addValues((float)69.2940599756579)
                                    .addValues((float)76.35206923995672)
                                    .addValues((float)83.41007850425555)
                                    .addValues((float)90.46808776855437)
                                    .build()
                    )
                            .addBinConfiguration(
                                    ProtoBuffSerializedBinConfiguration.newBuilder()
                                            .setFeatureName("visibility_surface")
                                            .addValues((float) -1759.9194969172195)
                                            .addValues((float)1662.3107932972966)
                                            .addValues((float)5084.541083511813)
                                            .addValues((float)8506.77137372633)
                                            .addValues((float)11929.001663940846)
                                            .addValues((float)15351.231954155359)
                                            .addValues((float)18773.462244369875)
                                            .addValues((float)22195.692534584392)
                                            .addValues((float)25617.92282479891)
                                            .build()
                            )
                            .addBinConfiguration(
                                    ProtoBuffSerializedBinConfiguration.newBuilder()
                                            .setFeatureName("temperature_surface")
                                            .addValues((float) 217.90924072265673)
                                            .addValues((float)227.61221064715863)
                                            .addValues((float)237.3151805716605)
                                            .addValues((float)247.0181504961624)
                                            .addValues((float)256.7211204206643)
                                            .addValues((float)266.4240903451662)
                                            .addValues((float)276.12706026966805)
                                            .addValues((float)285.8300301941699)
                                            .addValues((float)295.53300011867185)
                                            .addValues((float)305.2359700431737)
                                            .addValues((float)314.9389399676756)
                                            .addValues((float)324.6419098921775)
                                            .addValues((float)334.3448798166794)
                                            .build()
                            )
                            .addBinConfiguration(
                                    ProtoBuffSerializedBinConfiguration.newBuilder()
                                            .setFeatureName("relative_humidity_zerodegc_isotherm")
                                            .addValues((float) -14.999999999999963)
                                            .addValues((float)-6.80112890625014)
                                            .addValues((float)1.3977421874996825)
                                            .addValues((float)9.596613281249505)
                                            .addValues((float)17.795484374999326)
                                            .addValues((float)25.99435546874915)
                                            .addValues((float)34.19322656249897)
                                            .addValues((float)42.392097656248794)
                                            .addValues((float)50.590968749998616)
                                            .addValues((float)58.789839843748446)
                                            .addValues((float)66.98871093749827)
                                            .addValues((float)75.18758203124808)
                                            .addValues((float)83.38645312499791)
                                            .addValues((float)91.58532421874774)
                                            .addValues((float)99.78419531249756)
                                            .addValues((float)107.98306640624737)
                                            .addValues((float)116.1819374999972)
                                            .build()
                            )
                                            .build()
                            )
                            .build()
            )
            .build();




    public static void main(String[] args) throws IOException {
        if (args.length < 5) {
            System.out.println("Usage: dhtNodeAddress datasetId sessionId binConfigPath inputFiles...");
            return;
        }

        String dhtNodeAddress = args[0];
        String datasetId = args[1];
        long sessionId = Long.parseLong(args[2]);
        String binConfig = args[3];

        File baseDir = new File(args[4]);
        File[] inputFiles = baseDir.listFiles(
                pathname -> pathname.getName().startsWith("namanl_218_201501") && pathname.getName().endsWith(
                        "001.grb" + ".mblob"));

        if (inputFiles == null) {
            System.err.println("No matching files.");
            return;
        }

        System.out.println("Total matching file count: " + inputFiles.length);

        // sort based on timestamps to reduce the temporally adjacent data being fragmented over multiple files
        // this provides better temporal locality during query evaluations.
        Arrays.sort(inputFiles, (o1, o2) -> {
            String[] splits1 = o1.getName().split("_");
            String[] splits2 = o2.getName().split("_");
            if (splits1[2].equals(splits2[2])) { // same day, sort based on time
                return Integer.compare(Integer.parseInt(splits1[3]), Integer.parseInt(splits2[3]));
            }
            return Integer.compare(Integer.parseInt(splits1[2]), Integer.parseInt(splits2[2])); // sort based on day
        });

        Arrays.stream(inputFiles).forEach(f -> {
            System.out.println(f.getName());
        });

        /*File[] inputFiles = new File[args.length-4];
        for (int i = 4; i < args.length; i++) {
            inputFiles[i-4] = new File(args[i]);
        }*/

        for (int i = 0; i < Math.min(2, inputFiles.length); i++) {   // ingest data over two sessions
            SessionSchema sessionSchema =
                    new SessionSchema(Util.quantizerMapFromFile(binConfig), GEOHASH_LENGTH, TEMPORAL_BRACKET_LENGTH);

            StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId + i);
//        StrandPublisher strandPublisher = new DHTStrandPublisher(dhtNodeAddress, datasetId, sessionId);
//        StrandPublisher strandPublisher = new ConsoleStrandPublisher();

            StrandRegistry strandRegistry = new StrandRegistry(strandPublisher, 10000, 100);

            NoaaIngester noaaIngester = new NoaaIngester(
                    Arrays.copyOfRange(inputFiles, (int) Math.ceil(inputFiles.length / 2d) * i,
                                       (int) Math.ceil(inputFiles.length / 2d) * (i + 1)), sessionSchema);

            long timeStart = Instant.now().toEpochMilli();
            while (noaaIngester.hasNext()) {
                Strand strand = noaaIngester.next();
                if (strand != null) {
                    strandRegistry.add(strand);
                }
            }
            long totalStrandsPublished = strandRegistry.terminateSession();
            long timeEnd = Instant.now().toEpochMilli();
            double secondsElapsed = (timeEnd - timeStart) / 1000d;
            double strandsPerSec = totalStrandsPublished / secondsElapsed;

            System.out.println("Total Strands Published: " + totalStrandsPublished);
            System.out.printf("In Seconds: %.2f\n", secondsElapsed);
            System.out.printf("Strands per second: %.1f", strandsPerSec);
        }
    }
}
