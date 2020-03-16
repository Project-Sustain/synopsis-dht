package sustain.synopsis.dht.store.query;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.dht.store.StrandStorageKey;
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

        brackets2 = new ArrayList<>(Arrays.asList(new long[]{0, 100}));
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
        brackets1 = new ArrayList<>(Arrays.asList(new long[]{0, 100}));
        brackets2 = new ArrayList<>(Arrays.asList(new long[]{0, 10}, new long[]{9, 20}));
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(1, merged.size());
        Assertions.assertArrayEquals(new long[]{0, 20}, merged.get(0));

        // empty operands
        brackets1 = new ArrayList<>();
        merged = QueryUtil.mergeTemporalBracketsAsIntersect(brackets1, brackets2);
        Assertions.assertEquals(0, merged.size());
    }
}
