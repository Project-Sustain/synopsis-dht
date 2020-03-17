package sustain.synopsis.dht.store.query;

import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.sketch.dataset.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryUtil {
    /**
     * Filters out the {@link StrandStorageKey} objects that falls into the given temporal boundaries
     *
     * @param metadataMap       Map of {@link StrandStorageKey} objects arranged as a {@link NavigableMap}.
     * @param lowerBound        Lower bound of the temporal bracket, usually specified as an epoch, inclusive
     * @param upperBound        Upper bound of the temporal bracket, usually specified as an epoch, exclusive by default
     * @param includeUpperBound Whether to include any strand that has a starting ts equal to the upper bound
     * @param <T>               Value type of the map
     * @return Matching StrandStorageKeys and their associated values as a {@link NavigableMap}
     */
    public static <T> NavigableMap<StrandStorageKey, T> temporalLookup(NavigableMap<StrandStorageKey, T> metadataMap,
                                                                       long lowerBound, long upperBound,
                                                                       boolean includeUpperBound) {
        /* {@link StrandStorageKey} uses the 'from' attribute in the compare(). Therefore, we can use a dummy value
        as the 'to' attribute.
         */
        StrandStorageKey from = new StrandStorageKey(lowerBound, Long.MAX_VALUE);
        /* handle the case where lower bound is falling into the temporal bracket of a strand - subMap()
        skips that strand because lowerBound > strand.getStartTS(). Also make sure the strand.getEndTS() < lowerBound
        . */
        StrandStorageKey floorKey = metadataMap.floorKey(new StrandStorageKey(lowerBound, upperBound));
        if (floorKey != null && floorKey.getEndTS() > lowerBound) {
            from = floorKey;
        }

        StrandStorageKey to = new StrandStorageKey(upperBound, Long.MAX_VALUE);
        return metadataMap.subMap(from, true, to, includeUpperBound);
    }

    /**
     * Evaluate a temporal query expression against the given temporal scope
     *
     * @param expression      Temporal boundary defined using {@link Expression}
     * @param temporalBracket Current temporal scope. During the first call, it may correspond to the scope of the
     *                        available data. During subsequent invocations, the original scope may get reduced
     *                        after evaluating previous predicates/expressions.
     * @return ArrayList of matching temporal intervals
     * @throws QueryException If an unsupported combiner operation is used.
     */
    public static ArrayList<long[]> evaluateTemporalExpression(Expression expression, long[] temporalBracket) throws QueryException {
        ArrayList<long[]> scope1 = handleCase(expression.hasExpression1() ? expression.getExpression1() : null,
                expression.hasPredicate1() ? expression.getPredicate1() : null, temporalBracket);
        ArrayList<long[]> scope2 = handleCase(expression.hasExpression2() ? expression.getExpression2() : null,
                expression.hasPredicate2() ? expression.getPredicate2() : null, temporalBracket);
        return mergeResults(expression, scope1, scope2);
    }

    private static ArrayList<long[]> handleCase(Expression expression, Predicate predicate, long[] interval) throws QueryException {
        ArrayList<long[]> results = new ArrayList<>();
        if (expression != null) {
            results.addAll(evaluateTemporalExpression(expression, interval));
        } else if (predicate != null) {
            long[] result = evaluateTemporalPredicate(predicate, interval);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }

    private static ArrayList<long[]> mergeResults(Expression expression, ArrayList<long[]> scope1,
                                                  ArrayList<long[]> scope2) throws QueryException {
        // only one side of the expression has produced results. No need to run the combine operation.
        if (scope1.isEmpty() || scope2.isEmpty()) {
            scope1.addAll(scope2);
            return scope1;
        }

        Expression.CombineOperator combineOp = expression.getCombineOp();
        switch (combineOp) {
            case OR:
                scope1.addAll(scope2);
                mergeTemporalBracketsAsUnion(scope1);
                return scope1;
            case AND:
                return mergeTemporalBracketsAsIntersect(scope1, scope2);
            default: // we may support DIFF operator in the future
                throw new QueryException("Combine operator " + combineOp.toString() + " is not supported for " +
                        "temporal constraints.");
        }
    }

    /**
     * Evaluate temporal predicates against a given temporal scope
     *
     * @param predicate    Temporal constraint specified as a {@link Predicate}
     * @param currentScope Current temporal scope
     * @return Matching temporal scope if there is any, <code>Null</code> otherwise.
     */
    static long[] evaluateTemporalPredicate(Predicate predicate, long[] currentScope) {
        long parameter = predicate.getIntegerValue();
        long from = currentScope[0];
        long to = currentScope[1];
        switch (predicate.getComparisonOp()) {
            case GREATER_THAN:
                from = Math.max(parameter + 1, currentScope[0]);
                break;
            case GREATER_THAN_OR_EQUAL:
                from = Math.max(parameter, currentScope[0]);
                break;
            case LESS_THAN:
                to = Math.min(currentScope[1], parameter);
                break;
            case LESS_THAN_OR_EQUAL: // the upper bound is exclusive
                to = Math.min(currentScope[1], parameter + 1);
                break;
            case EQUAL:
                from = Math.max(parameter, currentScope[0]);
                to = Math.min(currentScope[1], parameter + 1);
                break;
        }
        if (from >= to) { // scope does not satisfy the constraint
            return null;
        }
        return new long[]{from, to};
    }

    /**
     * Calculate the union of a set of temporal brackets. If two temporal brackets are overlapping merge them into a
     * one interval. This is an in-place operation.
     *
     * @param brackets List of temporal brackets.
     */
    static void mergeTemporalBracketsAsUnion(ArrayList<long[]> brackets) {
        brackets.sort(Comparator.comparingLong(o -> o[0]));
        for (int i = 0; i < brackets.size() - 1; ) {
            long[] interval1 = brackets.get(i);
            long[] interval2 = brackets.get(i + 1);
            if (areOverlappingIntervals(interval1, interval2)) {
                // find the union between the two regions
                long[] merged = new long[]{Math.min(interval1[0], interval2[0]), Math.max(interval1[1], interval2[1])};
                brackets.remove(i);
                brackets.remove(i);
                brackets.add(i, merged);
            } else {
                i++;
            }
        }
    }

    /**
     * Given two groups of temporal brackets, calculate the list of brackets that have an intersecting region
     * between two entries from each groups.
     * Each temporal bracket from a group is matched against the every temporal bracket from the other group
     * to find intersecting regions.
     *
     * @param brackets1 first group of temporal brackets
     * @param brackets2 second group of temporal brackets
     * @return List of temporal brackets with intersecting intervals
     */
    static ArrayList<long[]> mergeTemporalBracketsAsIntersect(ArrayList<long[]> brackets1,
                                                              ArrayList<long[]> brackets2) {
        // consolidate each temporal bracket list to improve the efficiency
        mergeTemporalBracketsAsUnion(brackets1);
        mergeTemporalBracketsAsUnion(brackets2);
        ArrayList<long[]> mergedList =
                IntStream.range(0, Math.min(brackets1.size(), brackets2.size())).mapToObj(i -> new Pair<>(brackets1.get(i), brackets2.get(i))).filter(pair -> areOverlappingIntervals(pair.a, pair.b)).map(pair -> new long[]{Math.max(pair.a[0], pair.b[0]), Math.min(pair.a[1], pair.b[1])}).collect(Collectors.toCollection(ArrayList::new));
        // optimize the merged list to merge any overlapping intervals
        mergeTemporalBracketsAsUnion(mergedList);
        return mergedList;
    }

    static boolean areOverlappingIntervals(long[] interval1, long[] interval2) {
        if (interval1[0] > interval2[0]) {
            return interval2[1] > interval1[0];
        }
        if (interval2[0] > interval1[0]) {
            return interval1[1] > interval2[0];
        }
        return true; // interval1[0] == interval2[0];
    }
}
