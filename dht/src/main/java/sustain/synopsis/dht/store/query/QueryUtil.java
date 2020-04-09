package sustain.synopsis.dht.store.query;

import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.services.Expression;
import sustain.synopsis.dht.store.services.Predicate;
import sustain.synopsis.sketch.dataset.Pair;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class QueryUtil {
    /**
     * Filters out the {@link StrandStorageKey} objects that falls into the given temporal boundaries
     *
     * @param metadataMap       Map of {@link StrandStorageKey} objects arranged as a {@link NavigableMap}.
     * @param lowerBound        Lower bound of the temporal bracket, usually specified as an epoch, inclusive
     * @param upperBound        Upper bound of the temporal bracket, usually specified as an epoch, exclusive by
     *                          default
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
        // in case where block index has a single key
        if(metadataMap.size() == 1 && metadataMap.firstKey().getStartTS() <= upperBound){
            return metadataMap;
        }

        StrandStorageKey to = new StrandStorageKey(upperBound, Long.MAX_VALUE);
        return metadataMap.subMap(from, true, to, includeUpperBound);
    }

    /**
     * Evaluate a temporal query expression against the given temporal scope
     *
     * @param expression Temporal boundary defined using {@link Expression}
     * @param scope      Current temporal scope. During the first call, it may correspond to the scope of the available
     *                   data. During subsequent invocations, the original scope may get reduced after evaluating
     *                   previous predicates/expressions.
     * @return List of matching temporal intervals
     * @throws QueryException If an unsupported combiner operation is used.
     */
    public static List<Interval> evaluateTemporalExpression(Expression expression, Interval scope)
            throws QueryException {
        List<Interval> scope1 = handleCase(expression.hasExpression1() ? expression.getExpression1() : null,
                                           expression.hasPredicate1() ? expression.getPredicate1() : null, scope);
        List<Interval> scope2 = handleCase(expression.hasExpression2() ? expression.getExpression2() : null,
                                           expression.hasPredicate2() ? expression.getPredicate2() : null, scope);
        return mergeResults(expression, scope1, scope2);
    }

    private static List<Interval> handleCase(Expression expression, Predicate predicate, Interval scope)
            throws QueryException {
        ArrayList<Interval> results = new ArrayList<>();
        if (expression != null) {
            results.addAll(evaluateTemporalExpression(expression, scope));
        } else if (predicate != null) {
            Interval result = evaluateTemporalPredicate(predicate, scope);
            if (result != null) {
                results.add(result);
            }
        }
        return results;
    }

    private static List<Interval> mergeResults(Expression expression, List<Interval> scope1, List<Interval> scope2)
            throws QueryException {
        // only one side of the expression has produced results. No need to run the combine operation.
        if (scope1.isEmpty() || scope2.isEmpty()) {
            scope1.addAll(scope2);
            return scope1;
        }

        Expression.CombineOperator combineOp = expression.getCombineOp();
        switch (combineOp) {
            case OR:
                scope1.addAll(scope2);
                return mergeTemporalBracketsAsUnion(scope1);
            case AND:
                return mergeTemporalBracketsAsIntersect(scope1, scope2);
            default: // we may support DIFF operator in the future
                throw new QueryException("Combine operator " + combineOp.toString() + " is not supported for "
                                         + "temporal constraints.");
        }
    }

    /**
     * Evaluate temporal predicates against a given temporal scope
     *
     * @param predicate    Temporal constraint specified as a {@link Predicate}
     * @param currentScope Current temporal scope
     * @return Matching temporal scope if there is any, <code>Null</code> otherwise.
     */
    static Interval evaluateTemporalPredicate(Predicate predicate, Interval currentScope) throws QueryException {
        long parameter = predicate.getIntegerValue();
        long from = currentScope.getFrom();
        long to = currentScope.getTo();
        switch (predicate.getComparisonOp()) {
            case GREATER_THAN:
                from = Math.max(parameter + 1, currentScope.getFrom());
                break;
            case GREATER_THAN_OR_EQUAL:
                from = Math.max(parameter, currentScope.getFrom());
                break;
            case LESS_THAN:
                to = Math.min(currentScope.getTo(), parameter);
                break;
            case LESS_THAN_OR_EQUAL: // the upper bound is exclusive
                to = Math.min(currentScope.getTo(), parameter + 1);
                break;
            case EQUAL:
                from = Math.max(parameter, currentScope.getFrom());
                to = Math.min(currentScope.getTo(), parameter + 1);
                break;
            case UNRECOGNIZED:
                throw new QueryException("Unrecognized comparison operator: " + predicate.getComparisonOpValue());
        }
        return from >= to ? null : new Interval(from, to); // return null if scope does not satisfy the constraint
    }

    /**
     * Calculate the union of a set of temporal brackets. If two temporal brackets are overlapping merge them into a one
     * interval.
     *
     * @param brackets List of input intervals
     * @return Input intervals after merging
     */
    static List<Interval> mergeTemporalBracketsAsUnion(List<Interval> brackets) {
        List<Interval> result = new ArrayList<>(brackets);
        result.sort(Comparator.comparingLong(Interval::getFrom));
        for (int i = 0; i < result.size() - 1; ) {
            Interval interval1 = result.get(i);
            Interval interval2 = result.get(i + 1);
            if (interval1.isOverlapping(interval2)) {
                // find the union between the two regions
                Interval merged = new Interval(Math.min(interval1.getFrom(), interval2.getFrom()),
                                               Math.max(interval1.getTo(), interval2.getTo()));
                result.remove(i);
                result.remove(i);
                result.add(i, merged);
            } else {
                i++;
            }
        }
        return result;
    }

    /**
     * Given two groups of temporal brackets, calculate the list of brackets that have an intersecting region between
     * two entries from each groups. Each temporal bracket from a group is matched against the every temporal bracket
     * from the other group to find intersecting regions.
     *
     * @param brackets1 first group of temporal brackets
     * @param brackets2 second group of temporal brackets
     * @return List of temporal brackets with intersecting intervals
     */
    static List<Interval> mergeTemporalBracketsAsIntersect(List<Interval> brackets1, List<Interval> brackets2) {
        // consolidate each temporal bracket list to improve the efficiency
        List<Interval> processedBrackets1 = mergeTemporalBracketsAsUnion(brackets1);
        List<Interval> processedBrackets2 = mergeTemporalBracketsAsUnion(brackets2);
        List<Interval> mergedList = IntStream.range(0, Math.min(processedBrackets1.size(), processedBrackets2.size()))
                                             .mapToObj(i -> new Pair<>(processedBrackets1.get(i),
                                                                       processedBrackets2.get(i)))
                                             .filter(pair -> pair.a.isOverlapping(pair.b))
                                             .map(pair -> new Interval(Math.max(pair.a.getFrom(), pair.b.getFrom()),
                                                                       Math.min(pair.a.getTo(), pair.b.getTo())))
                                             .collect(Collectors.toList());
        // optimize the merged list to merge any overlapping intervals
        return mergeTemporalBracketsAsUnion(mergedList);
    }
}
