package sustain.synopsis.dht.store.query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
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
