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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.dht.services.query.Interval;
import sustain.synopsis.dht.services.query.MatchedSSTable;
import sustain.synopsis.dht.services.query.QueryException;
import sustain.synopsis.dht.services.query.QueryUtil;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.util.*;

public class QueryUtilTest {
    @Test
    void testTemporalLookup() {
        TreeMap<StrandStorageKey, Metadata<StrandStorageKey>> metadata = new TreeMap<>();
        Metadata<StrandStorageKey> emptyMetadata = new Metadata<>();
        metadata.put(new StrandStorageKey(100L, 500L), emptyMetadata);
        metadata.put(new StrandStorageKey(400L, 1000L), emptyMetadata);
        metadata.put(new StrandStorageKey(1000L, 1500L), emptyMetadata);
        metadata.put(new StrandStorageKey(1600L, 2000L), emptyMetadata);

        Map<StrandStorageKey, Metadata<StrandStorageKey>> results = QueryUtil.temporalLookup(metadata, 0L, 400L, false);
        Map<StrandStorageKey, Metadata<StrandStorageKey>> expected =
                Collections.singletonMap(new StrandStorageKey(100L, 500L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = QueryUtil.temporalLookup(metadata, 0L, 400L, true);
        expected = new TreeMap<>();
        expected.put(new StrandStorageKey(100L, 500L), emptyMetadata);
        expected.put(new StrandStorageKey(400L, 1000L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = QueryUtil.temporalLookup(metadata, 150L, 350L, false);
        expected = Collections.singletonMap(new StrandStorageKey(100L, 500L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = QueryUtil.temporalLookup(metadata, 50, 550, true);
        expected = new TreeMap<>();
        expected.put(new StrandStorageKey(100L, 500L), emptyMetadata);
        expected.put(new StrandStorageKey(400L, 1000L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = QueryUtil.temporalLookup(metadata, 1400L, 2500L, false);
        expected = new TreeMap<>();
        expected.put(new StrandStorageKey(1000L, 1500L), emptyMetadata);
        expected.put(new StrandStorageKey(1600L, 2000L), emptyMetadata);
        Assertions.assertEquals(expected, results);


        results = QueryUtil.temporalLookup(metadata, 1750L, 1900L, false);
        expected = Collections.singletonMap(new StrandStorageKey(1600L, 2000L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        // edge cases
        results = QueryUtil.temporalLookup(metadata, 2000L, 3000L, false);
        Assertions.assertTrue(results.isEmpty());

        results = QueryUtil.temporalLookup(metadata, 0L, 100L, false);
        Assertions.assertTrue(results.isEmpty());

        results = QueryUtil.temporalLookup(metadata, 0L, 100L, true);
        expected = Collections.singletonMap(new StrandStorageKey(100L, 500L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        // cover the entire map
        results = QueryUtil.temporalLookup(metadata, 50L, 2000L, true);
        Assertions.assertEquals(metadata, results);

        // both from and to below the lowest key
        results = QueryUtil.temporalLookup(metadata, 0L, 50L, false);
        Assertions.assertTrue(results.isEmpty());

        // both from and to keys are higher than the highest key
        results = QueryUtil.temporalLookup(metadata, 2050L, 3000L, false);
        Assertions.assertTrue(results.isEmpty());

        // empty map
        results = QueryUtil.temporalLookup(new TreeMap<>(), 2050L, 3000L, false);
        Assertions.assertTrue(results.isEmpty());

        metadata.clear();
        metadata.put(new StrandStorageKey(0L, 50), emptyMetadata); // creates a gap from [50 - 100)
        metadata.put(new StrandStorageKey(100L, 1000L), emptyMetadata);
        metadata.put(new StrandStorageKey(1000L, 1500L), emptyMetadata);
        metadata.put(new StrandStorageKey(1600L, 2000L), emptyMetadata);
        results = QueryUtil.temporalLookup(metadata, 60L, 80L, false);
        Assertions.assertTrue(results.isEmpty());
    }

    @Test
    void testTemporalLookupWithOneBlock() {
        TreeMap<StrandStorageKey, Metadata<StrandStorageKey>> metadata = new TreeMap<>();
        Metadata<StrandStorageKey> emptyMetadata = new Metadata<>();
        metadata.put(new StrandStorageKey(100L, 500L), emptyMetadata);
        Map<StrandStorageKey, Metadata<StrandStorageKey>> results =
                QueryUtil.temporalLookup(metadata, 800L, 1600L, false);
        Assertions.assertFalse(results.isEmpty());

        results = QueryUtil.temporalLookup(metadata, 10L, 50L, false);
        Assertions.assertTrue(results.isEmpty());
    }

    @Test
    void IntervalBasicTest() {
        Interval interval1 = new Interval(10, 20);
        Interval interval2 = new Interval(10, 20);
        Interval interval3 = new Interval(10, 40);
        Interval interval4 = new Interval(0, 20);

        Assertions.assertEquals(10, interval1.getFrom());
        Assertions.assertEquals(20, interval1.getTo());

        Assertions.assertEquals(interval1, interval1);
        Assertions.assertEquals(interval1, interval2);
        Assertions.assertEquals(interval1.hashCode(), interval2.hashCode());
        Assertions.assertNotEquals(interval1, new Object());
        Assertions.assertNotEquals(interval1, interval3);
        Assertions.assertNotEquals(interval1, interval4);
    }

    @Test
    void testOverlappingIntervals() {
        // this method is supposed to be commutative
        Assertions.assertFalse(new Interval(0, 10).isOverlapping(new Interval(11, 15)));
        Assertions.assertFalse(new Interval(11, 15).isOverlapping(new Interval(0, 10)));

        Assertions.assertFalse(new Interval(0, 10).isOverlapping(new Interval(10, 15)));

        Assertions.assertTrue(new Interval(0, 10).isOverlapping(new Interval(8, 11)));
        Assertions.assertTrue(new Interval(8, 11).isOverlapping(new Interval(0, 10)));

        Assertions.assertTrue(new Interval(0, 10).isOverlapping(new Interval(5, 8)));
        Assertions.assertTrue(new Interval(5, 8).isOverlapping(new Interval(0, 10)));

        Assertions.assertTrue(new Interval(0, 10).isOverlapping(new Interval(0, 5L)));
        Assertions.assertTrue(new Interval(0, 5).isOverlapping(new Interval(0, 10)));
    }

    @Test
    void testMergeTemporalBracketsAsUnion() {
        List<Interval> brackets = new ArrayList<>(
                Arrays.asList(new Interval(0, 100), new Interval(200, 300), new Interval(250, 350),
                              new Interval(340, 400), new Interval(380, 390)));
        List<Interval> result = QueryUtil.mergeTemporalBracketsAsUnion(brackets);
        Assertions.assertEquals(Arrays.asList(new Interval(0, 100), new Interval(200, 400)), result);
    }

    @Test
    void testMergeTemporalBracketsAsIntersect() {
        List<Interval> brackets1 = new ArrayList<>(Arrays.asList(new Interval(0, 100), new Interval(200, 300)));
        List<Interval> brackets2 = new ArrayList<>(Arrays.asList(new Interval(80, 120), new Interval(220, 240)));
        List<Interval> merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(new ArrayList<>(Arrays.asList(new Interval(80, 100), new Interval(220, 240))), merged);

        brackets2 = new ArrayList<>(Collections.singletonList(new Interval(0, 100)));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(Collections.singletonList(new Interval(0, 100)), merged);

        // ensure consolidation happens in the operands before merging
        brackets1 = new ArrayList<>(Arrays.asList(new Interval(0, 100), new Interval(10, 30)));
        brackets2 = new ArrayList<>(Arrays.asList(new Interval(0, 10), new Interval(9, 20)));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(Collections.singletonList(new Interval(0, 20)), merged);

        // ensure consolidation happens in the output
        brackets1 = new ArrayList<>(Collections.singletonList(new Interval(0, 100)));
        brackets2 = new ArrayList<>(Arrays.asList(new Interval(0, 10), new Interval(9, 20)));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(Collections.singletonList(new Interval(0, 20)), merged);

        // empty operands
        brackets1 = new ArrayList<>();
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(0, merged.size());
    }

    @Test
    void testEvaluateTemporalPredicateLessThan() throws QueryException {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.LESS_THAN)
                         .build();
        Assertions.assertEquals(new Interval(0, 1000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 2000)));
        Assertions.assertEquals(new Interval(0, 200),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 200)));
        Assertions.assertEquals(new Interval(0, 1000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 1000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1000, 2000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1200, 2000)));
    }

    @Test
    void testEvaluateTemporalPredicateLessThanOrEqual() throws QueryException {
        Predicate predicate = Predicate.newBuilder().setIntegerValue(1000)
                                       .setComparisonOp(Predicate.ComparisonOperator.LESS_THAN_OR_EQUAL).build();
        Assertions.assertEquals(new Interval(0, 1001),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 2000)));
        Assertions.assertEquals(new Interval(0, 200),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 200)));
        Assertions.assertEquals(new Interval(0, 1000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 1000)));
        Assertions.assertEquals(new Interval(1000, 1001),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1000, 2000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1200, 2000)));
    }

    @Test
    void testEvaluateTemporalPredicateGreaterThan() throws QueryException {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN)
                         .build();
        Assertions.assertEquals(new Interval(1001, 2000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 2000)));
        Assertions.assertEquals(new Interval(1001, 2000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1000, 2000)));
        Assertions.assertEquals(new Interval(1500, 2000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1500, 2000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 1000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 100)));
    }

    @Test
    void testEvaluateTemporalPredicateGreaterThanOrEqual() throws QueryException {
        Predicate predicate = Predicate.newBuilder().setIntegerValue(1000)
                                       .setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL).build();
        Assertions.assertEquals(new Interval(1000, 2000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 2000)));
        Assertions.assertEquals(new Interval(1000, 2000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1000, 2000)));
        Assertions.assertEquals(new Interval(1500, 2000),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1500, 2000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 1000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 100)));
    }

    @Test
    void testEvaluateTemporalPredicateEqual() throws QueryException {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.EQUAL)
                         .build();
        Assertions.assertEquals(new Interval(1000, 1001),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 2000)));
        Assertions.assertEquals(new Interval(1000, 1001),
                                QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1000, 1100)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 1000)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 100)));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new Interval(1100, 1200)));
    }

    @Test
    void testEvaluateTemporalPredicateWithUnsupportedComparison() {
        Predicate predicate = Predicate.newBuilder().setIntegerValue(1000)
                                       .setComparisonOpValue(Predicate.ComparisonOperator.values().length + 1).build();
        Assertions.assertThrows(QueryException.class,
                                () -> QueryUtil.evaluateTemporalPredicate(predicate, new Interval(0, 2000)));
    }

    @Test
    void testEvaluateTemporalExpressionWithPredicates() throws QueryException {
        // 1000 < t
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1000)
                         .build();
        // t < 2000
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(2000)
                         .build();

        // one sided expression: 1000 < t
        Expression temporalExpression = Expression.newBuilder().setPredicate1(predicate1).build();
        List<Interval> result = QueryUtil.evaluateTemporalExpression(temporalExpression, new Interval(0, 5000));
        Assertions.assertEquals(Collections.singletonList(new Interval(1001, 5000)), result);

        // two sides expression with two predicates: 1000 < t < 2000 (using AND)
        temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(predicate2).build();
        result = QueryUtil.evaluateTemporalExpression(temporalExpression, new Interval(0, 5000));
        Assertions.assertEquals(Collections.singletonList(new Interval(1001, 2000)), result);

        // two sides expression with two predicates: 1000 > t OR t > 2000 (using OR)
        predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(1000)
                         .build();
        predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(2000)
                         .build();
        temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.OR)
                          .setPredicate2(predicate2).build();
        result = QueryUtil.evaluateTemporalExpression(temporalExpression, new Interval(0, 5000));
        Assertions.assertEquals(Arrays.asList(new Interval(0, 1000), new Interval(2001, 5000)), result);
    }

    @Test
    void testEvaluateTemporalExpressionWithExpressions() throws QueryException {
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1000)
                         .build();
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(2000)
                         .build();
        // two sides expression with two predicates: 1000 < t < 2000
        Expression temporalExpression1 =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(predicate2).build();

        Predicate predicate3 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1500)
                         .build();
        Predicate predicate4 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(3000)
                         .build();
        // two sides expression with two predicates: 1500 < t < 3000
        Expression temporalExpression2 =
                Expression.newBuilder().setPredicate1(predicate3).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(predicate4).build();

        // combined expression with AND
        Expression combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression1).setCombineOp(Expression.CombineOperator.AND)
                          .setExpression2(temporalExpression2).build();
        List<Interval> result = QueryUtil.evaluateTemporalExpression(combinedExpression, new Interval(0, 5000));
        Assertions.assertEquals(Collections.singletonList(new Interval(1501, 2000)), result);

        // combined expression with OR
        combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression1).setCombineOp(Expression.CombineOperator.OR)
                          .setExpression2(temporalExpression2).build();
        result = QueryUtil.evaluateTemporalExpression(combinedExpression, new Interval(0, 5000));
        Assertions.assertEquals(Collections.singletonList(new Interval(1001, 3000)), result);

        // unsupported combine operator
        Expression finalCombinedExpression = Expression.newBuilder().setExpression1(temporalExpression1)
                                                       .setCombineOp(Expression.CombineOperator.DIFF)
                                                       .setExpression2(temporalExpression2).build();
        Assertions.assertThrows(QueryException.class, () -> QueryUtil
                .evaluateTemporalExpression(finalCombinedExpression, new Interval(0, 5000)));
    }

    @Test
    void testEvaluateTemporalExpressionWithExpressionsAndPredicates() throws QueryException {
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1000)
                         .build();
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(2000)
                         .build();
        // 1500 < t < 3000
        Expression temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(predicate2).build();
        // t > 4500
        Predicate predicate3 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(4500)
                         .build();

        Expression combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(predicate3).build();
        List<Interval> result = QueryUtil.evaluateTemporalExpression(combinedExpression, new Interval(0, 5000));
        Assertions.assertEquals(0, result.size());

        combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression).setCombineOp(Expression.CombineOperator.OR)
                          .setPredicate2(predicate3).build();
        result = QueryUtil.evaluateTemporalExpression(combinedExpression, new Interval(0, 5000));
        Assertions.assertEquals(Arrays.asList(new Interval(1001, 2000), new Interval(4501, 5000)), result);
    }

    @Test
    void testEvaluateTemporalExpressionWithNonMatchingPredicates() throws QueryException {
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(10000)
                         .build();
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(1000)
                         .build();
        // 1500 < t < 3000
        Expression temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND)
                          .setPredicate2(predicate2).build();

        List<Interval> result = QueryUtil.evaluateTemporalExpression(temporalExpression, new Interval(1200, 5000));
        Assertions.assertEquals(0, result.size());
    }

    @Test
    void testMatchedSSTable() {
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        MatchedSSTable matchedSSTable = new MatchedSSTable(metadata);
        matchedSSTable.addMatchedInterval(new Interval(100, 200));
        matchedSSTable.addMatchedInterval(new Interval(200, 300));

        Assertions.assertEquals(metadata, matchedSSTable.getMetadata()); // object comparison
        Assertions.assertEquals(Arrays.asList(new Interval(100, 200), new Interval(200, 300)),
                                matchedSSTable.getMatchedIntervals());
    }
}
