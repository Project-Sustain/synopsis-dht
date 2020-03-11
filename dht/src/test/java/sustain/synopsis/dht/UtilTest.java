package sustain.synopsis.dht;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

public class UtilTest {
    @Test
    void testTemporalLookup() {
        TreeMap<StrandStorageKey, Metadata<StrandStorageKey>> metadata = new TreeMap<>();
        Metadata<StrandStorageKey> emptyMetadata = new Metadata<>();
        metadata.put(new StrandStorageKey(100, 500), emptyMetadata);
        metadata.put(new StrandStorageKey(400, 1000), emptyMetadata);
        metadata.put(new StrandStorageKey(1000, 1500), emptyMetadata);
        metadata.put(new StrandStorageKey(1600, 2000), emptyMetadata);

        Map<StrandStorageKey, Metadata<StrandStorageKey>> results = Util.temporalLookup(metadata, 0L, 400L,
                false);
        Map<StrandStorageKey, Metadata<StrandStorageKey>> expected =
                Collections.singletonMap(new StrandStorageKey(100L, 500L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = Util.temporalLookup(metadata, 0L, 400L, true);
        expected = new TreeMap<>();
        expected.put(new StrandStorageKey(100L, 500L), emptyMetadata);
        expected.put(new StrandStorageKey(400L, 1000L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = Util.temporalLookup(metadata, 150L, 350L, false);
        expected = Collections.singletonMap(new StrandStorageKey(100L, 500L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        results = Util.temporalLookup(metadata, 50L, 550L, true);
        expected = new TreeMap<>();
        expected.put(new StrandStorageKey(100L, 500L), emptyMetadata);
        expected.put(new StrandStorageKey(400L, 1000L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        // edge cases
        results = Util.temporalLookup(metadata, 2000, 3000L, false);
        Assertions.assertTrue(results.isEmpty());

        results = Util.temporalLookup(metadata, 0, 100L, false);
        Assertions.assertTrue(results.isEmpty());

        results = Util.temporalLookup(metadata, 0, 100L, true);
        expected = Collections.singletonMap(new StrandStorageKey(100L, 500L), emptyMetadata);
        Assertions.assertEquals(expected, results);

        // cover the entire map
        results = Util.temporalLookup(metadata, 50L, 2000L, true);
        Assertions.assertEquals(metadata, results);

        // both from and to below the lowest key
        results = Util.temporalLookup(metadata, 0L, 50L, false);
        Assertions.assertTrue(results.isEmpty());

        // both from and to keys are higher than the highest key
        results = Util.temporalLookup(metadata, 2050, 3000, false);
        Assertions.assertTrue(results.isEmpty());

        // empty map
        results = Util.temporalLookup(new TreeMap<>(), 2050, 3000, false);
        Assertions.assertTrue(results.isEmpty());
    }
}
