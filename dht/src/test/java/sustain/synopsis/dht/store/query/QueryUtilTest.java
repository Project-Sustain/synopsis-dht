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
    void testOverlappingIntervals() {
        // this method is supposed to be commutative
        Assertions.assertFalse(QueryUtil.areOverlappingIntervals(new long[]{0, 10}, new long[]{11, 15}));
        Assertions.assertFalse(QueryUtil.areOverlappingIntervals(new long[]{11, 15}, new long[]{0, 10}));

        Assertions.assertFalse(QueryUtil.areOverlappingIntervals(new long[]{0, 10}, new long[]{10, 15}));

        Assertions.assertTrue(QueryUtil.areOverlappingIntervals(new long[]{0, 10}, new long[]{8, 11}));
        Assertions.assertTrue(QueryUtil.areOverlappingIntervals(new long[]{8, 11}, new long[]{0, 10}));

        Assertions.assertTrue(QueryUtil.areOverlappingIntervals(new long[]{0, 10}, new long[]{5, 8}));
        Assertions.assertTrue(QueryUtil.areOverlappingIntervals(new long[]{5, 8}, new long[]{0, 10}));

        Assertions.assertTrue(QueryUtil.areOverlappingIntervals(new long[]{0, 10}, new long[]{0, 5}));
        Assertions.assertTrue(QueryUtil.areOverlappingIntervals(new long[]{0, 5}, new long[]{0, 10}));
    }

    @Test
    void testMergeTemporalBracketsAsUnion() {
        ArrayList<long[]> brackets = new ArrayList<>(Arrays.asList(new long[]{0, 100}, new long[]{200, 300},
                new long[]{250, 350}, new long[]{340, 400}, new long[]{380, 390}));
        QueryUtil.mergeTemporalBracketsAsUnion(brackets);
        Assertions.assertEquals(2, brackets.size());
        Assertions.assertArrayEquals(brackets.get(0), new long[]{0, 100});
        Assertions.assertArrayEquals(brackets.get(1), new long[]{200, 400});
    }

    @Test
    void testMergeTemporalBracketsAsIntersect() {
        ArrayList<long[]> brackets1 = new ArrayList<>(Arrays.asList(new long[]{0, 100}, new long[]{200, 300}));
        ArrayList<long[]> brackets2 = new ArrayList<>(Arrays.asList(new long[]{80, 120}, new long[]{220, 240}));
        ArrayList<long[]> merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(2, merged.size());
        Assertions.assertArrayEquals(new long[]{80, 100}, merged.get(0));
        Assertions.assertArrayEquals(new long[]{220, 240}, merged.get(1));

        brackets2 = new ArrayList<>(Collections.singletonList(new long[]{0, 100}));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(1, merged.size());
        Assertions.assertArrayEquals(new long[]{0, 100}, merged.get(0));

        // ensure consolidation happens in the operands before merging
        brackets1 = new ArrayList<>(Arrays.asList(new long[]{0, 100}, new long[]{10, 30}));
        brackets2 = new ArrayList<>(Arrays.asList(new long[]{0, 10}, new long[]{9, 20}));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(1, merged.size());
        Assertions.assertArrayEquals(new long[]{0, 20}, merged.get(0));

        // ensure consolidation happens in the output
        brackets1 = new ArrayList<>(Collections.singletonList(new long[]{0, 100}));
        brackets2 = new ArrayList<>(Arrays.asList(new long[]{0, 10}, new long[]{9, 20}));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(1, merged.size());
        Assertions.assertArrayEquals(new long[]{0, 20}, merged.get(0));

        // empty operands
        brackets1 = new ArrayList<>();
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(0, merged.size());
    }

    @Test
    void testEvaluateTemporalPredicateLessThan() {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).build();
        Assertions.assertArrayEquals(new long[]{0, 1000}, QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0
                , 2000}));
        Assertions.assertArrayEquals(new long[]{0, 200}, QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0,
                200}));
        Assertions.assertArrayEquals(new long[]{0, 1000}, QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0
                , 1000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{1000, 2000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{1200, 2000}));
    }

    @Test
    void testEvaluateTemporalPredicateLessThanOrEqual() {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.LESS_THAN_OR_EQUAL).build();
        Assertions.assertArrayEquals(new long[]{0, 1001}, QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0
                , 2000}));
        Assertions.assertArrayEquals(new long[]{0, 200}, QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0,
                200}));
        Assertions.assertArrayEquals(new long[]{0, 1000}, QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0
                , 1000}));
        Assertions.assertArrayEquals(new long[]{1000, 1001}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{1000, 2000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{1200, 2000}));
    }

    @Test
    void testEvaluateTemporalPredicateGreaterThan() {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).build();
        Assertions.assertArrayEquals(new long[]{1001, 2000}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{0, 2000}));
        Assertions.assertArrayEquals(new long[]{1001, 2000}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{1000, 2000}));
        Assertions.assertArrayEquals(new long[]{1500, 2000}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{1500, 2000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0, 1000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0, 100}));
    }

    @Test
    void testEvaluateTemporalPredicateGreaterThanOrEqual() {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN_OR_EQUAL).build();
        Assertions.assertArrayEquals(new long[]{1000, 2000}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{0, 2000}));
        Assertions.assertArrayEquals(new long[]{1000, 2000}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{1000, 2000}));
        Assertions.assertArrayEquals(new long[]{1500, 2000}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{1500, 2000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0, 1000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0, 100}));
    }

    @Test
    void testEvaluateTemporalPredicateEqual() {
        Predicate predicate =
                Predicate.newBuilder().setIntegerValue(1000).setComparisonOp(Predicate.ComparisonOperator.EQUAL).build();
        Assertions.assertArrayEquals(new long[]{1000, 1001}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{0, 2000}));
        Assertions.assertArrayEquals(new long[]{1000, 1001}, QueryUtil.evaluateTemporalPredicate(predicate,
                new long[]{1000, 1100}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0, 1000}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{0, 100}));
        Assertions.assertNull(QueryUtil.evaluateTemporalPredicate(predicate, new long[]{1100, 1200}));
    }

    @Test
    void testEvaluateTemporalExpressionWithPredicates() throws QueryException {
        // 1000 < t
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1000).build();
        // t < 2000
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(2000).build();

        // one sided expression: 1000 < t
        Expression temporalExpression = Expression.newBuilder().setPredicate1(predicate1).build();
        List<long[]> result = QueryUtil.evaluateTemporalExpression(temporalExpression, new long[]{0, 5000});
        Assertions.assertEquals(1, result.size());
        Assertions.assertArrayEquals(new long[]{1001, 5000}, result.get(0));

        // two sides expression with two predicates: 1000 < t < 2000 (using AND)
        temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND).setPredicate2(predicate2).build();
        result = QueryUtil.evaluateTemporalExpression(temporalExpression, new long[]{0, 5000});
        Assertions.assertEquals(1, result.size());
        Assertions.assertArrayEquals(new long[]{1001, 2000}, result.get(0));

        // two sides expression with two predicates: 1000 > t OR t > 2000 (using OR)
        predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(1000).build();
        predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(2000).build();
        temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.OR).setPredicate2(predicate2).build();
        result = QueryUtil.evaluateTemporalExpression(temporalExpression, new long[]{0, 5000});
        Assertions.assertEquals(2, result.size());
        Assertions.assertArrayEquals(new long[]{0, 1000}, result.get(0));
        Assertions.assertArrayEquals(new long[]{2001, 5000}, result.get(1));

    }

    @Test
    void testEvaluateTemporalExpressionWithExpressions() throws QueryException {
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1000).build();
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(2000).build();
        // two sides expression with two predicates: 1000 < t < 2000
        Expression temporalExpression1 =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND).setPredicate2(predicate2).build();

        Predicate predicate3 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1500).build();
        Predicate predicate4 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(3000).build();
        // two sides expression with two predicates: 1500 < t < 3000
        Expression temporalExpression2 =
                Expression.newBuilder().setPredicate1(predicate3).setCombineOp(Expression.CombineOperator.AND).setPredicate2(predicate4).build();

        // combined expression with AND
        Expression combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression1).setCombineOp(Expression.CombineOperator.AND).setExpression2(temporalExpression2).build();
        List<long[]> result = QueryUtil.evaluateTemporalExpression(combinedExpression, new long[]{0, 5000});
        Assertions.assertEquals(1, result.size());
        Assertions.assertArrayEquals(new long[]{1501, 2000}, result.get(0));

        // combined expression with OR
        combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression1).setCombineOp(Expression.CombineOperator.OR).setExpression2(temporalExpression2).build();
        result = QueryUtil.evaluateTemporalExpression(combinedExpression, new long[]{0, 5000});
        Assertions.assertEquals(1, result.size());
        Assertions.assertArrayEquals(new long[]{1001, 3000}, result.get(0));

        // unsupported combine operator
        Expression finalCombinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression1).setCombineOp(Expression.CombineOperator.DIFF).setExpression2(temporalExpression2).build();
        Assertions.assertThrows(QueryException.class, ()-> QueryUtil.evaluateTemporalExpression(finalCombinedExpression,
                new long[]{0, 5000}));
    }

    @Test
    void testEvaluateTemporalExpressionWithExpressionsAndPredicates() throws QueryException {
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(1000).build();
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(2000).build();
        // 1500 < t < 3000
        Expression temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND).setPredicate2(predicate2).build();
        // t > 4500
        Predicate predicate3 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(4500).build();

        Expression combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression).setCombineOp(Expression.CombineOperator.AND).setPredicate2(predicate3).build();
        List<long[]> result = QueryUtil.evaluateTemporalExpression(combinedExpression, new long[]{0, 5000});
        Assertions.assertEquals(0, result.size());

        combinedExpression =
                Expression.newBuilder().setExpression1(temporalExpression).setCombineOp(Expression.CombineOperator.OR).setPredicate2(predicate3).build();
        result = QueryUtil.evaluateTemporalExpression(combinedExpression, new long[]{0, 5000});
        Assertions.assertEquals(2, result.size());
        Assertions.assertArrayEquals(new long[]{1001, 2000}, result.get(0));
        Assertions.assertArrayEquals(new long[]{4501, 5000}, result.get(1));
    }

    @Test
    void testEvaluateTemporalExpressionWithNonMatchingPredicates() throws QueryException {
        Predicate predicate1 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.GREATER_THAN).setIntegerValue(10000).build();
        Predicate predicate2 =
                Predicate.newBuilder().setComparisonOp(Predicate.ComparisonOperator.LESS_THAN).setIntegerValue(1000).build();
        // 1500 < t < 3000
        Expression temporalExpression =
                Expression.newBuilder().setPredicate1(predicate1).setCombineOp(Expression.CombineOperator.AND).setPredicate2(predicate2).build();

        List<long[]> result = QueryUtil.evaluateTemporalExpression(temporalExpression, new long[]{1200, 5000});
        Assertions.assertEquals(0, result.size());
    }
}
