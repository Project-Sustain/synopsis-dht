package sustain.synopsis.ingestion.client.core;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.sketch.dataset.Quantizer;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.Path;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static org.junit.jupiter.api.Assertions.*;

class IngestionTest {

    @Mock
    private StrandRegistry registryMock;

    @Mock
    private StrandPublisher publisherMock;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void testRecordToStrandConversion() {
        Map<String, Quantizer> quantizers = new HashMap<>();
        quantizers.put("feature_1", new Quantizer(new Feature(0.0), new Feature(10.0), new Feature(20.0)));
        quantizers.put("feature_2", new Quantizer(new Feature(100.0), new Feature(200.0), new Feature(1000.0)));

        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(Duration.ofHours(1));
        IngestionTask ingestionTask = new IngestionTask(null, null, quantizers, temporalQuantizer);
        Record record = new Record();
        record.setGeohash("9xj");
        long ts = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 23));
        record.setTimestamp(ts);
        record.addFeatureValue("feature_1", 3.0);
        record.addFeatureValue("feature_2", 140.0);
        Strand strand = ingestionTask.convertToStrand(record);

        //  check the spatio-temporal attributes
        long[] boundaries = temporalQuantizer.getTemporalBoundaries(ts);
        assertEquals(boundaries[0], strand.getFromTimeStamp());
        assertEquals(boundaries[1], strand.getToTimestamp());
        assertEquals("9xj", strand.getGeohash());

        // check the path
        Path path = strand.getPath();
        assertEquals("feature_1", path.get(0).getLabel().getName());
        assertEquals(0.0, path.get(0).getLabel().getDouble()); // discretized value

        assertEquals("feature_2", path.get(1).getLabel().getName());
        assertEquals(100.0, path.get(1).getLabel().getDouble()); // discretized value
        assertNotNull(path.get(1).getData()); // check the data container

        // add a new feature that does not have a quantizer
        record.addFeatureValue("feature_3", 3.01);
        assertThrows(AssertionError.class, () -> {
            ingestionTask.convertToStrand(record);
        });
    }

    @Test
    void testIngestionTaskExecution() {
        Map<String, Quantizer> quantizers = new HashMap<>();
        quantizers.put("feature_1", new Quantizer(new Feature(0.0), new Feature(10.0), new Feature(20.0)));
        quantizers.put("feature_2", new Quantizer(new Feature(100.0), new Feature(200.0), new Feature(1000.0)));

        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(Duration.ofHours(1));

        ArrayBlockingQueue<Record> input = new ArrayBlockingQueue<>(5);
        IngestionTask ingestionTask = new IngestionTask(registryMock, input, quantizers, temporalQuantizer);
        Thread t = new Thread(ingestionTask);
        t.start();

        Record r1 = new Record();
        r1.setGeohash("9xj");
        r1.setTimestamp(TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 23)));
        r1.addFeatureValue("feature_1", 20.0);
        r1.addFeatureValue("feature_2", 200.0);

        Record r2 = new Record();
        r2.setGeohash("9xj");
        r2.setTimestamp(TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 24)));
        r2.addFeatureValue("feature_1", 20.0);
        r2.addFeatureValue("feature_2", 200.0);

        input.add(r1);
        input.add(r2);
        // allow a few seconds for the thread to pick up the data - there is a 5 second wait if the queue is empty
        // setting a longer timeout to avoid false positive test failures due to a slower process
        Mockito.verify(registryMock, Mockito.timeout(20 * 1000).times(2)).add(Mockito.any(Strand.class));

        ingestionTask.terminate();
        Mockito.verify(registryMock, Mockito.timeout(20 * 1000).times(1)).terminateSession();
    }

    @Test
    void testStrandRegistryAdd() {
        LocalDateTime from = LocalDateTime.of(2019, 2, 12, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2019, 2, 12, 1, 1, 0);
        Strand strand1 = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.0, 2.0);
        StrandRegistry registry = new StrandRegistry(strands -> {
            //ignore
        });
        assertEquals(1, registry.add(strand1));

        // add a different strand
        Strand strand2 = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.1, 2.0);
        assertEquals(2, registry.add(strand2));

        // add a strand for merging
        Strand similarStrand = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.0, 2.0);
        assertEquals(2, registry.add(similarStrand));
    }

    @Test
    void testStrandRegistryPrefixFiltering(){
        LocalDateTime from = LocalDateTime.of(2019, 2, 12, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2019, 2, 12, 1, 1, 0);
        Strand strand1 = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.0, 2.0);
        Strand strand2 = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.1, 2.0);
        // different geohash
        Strand strand3 = TestUtil.createStrand(new Path(), "9xi", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.0, 2.0);

        // increments the timestamp for the prefix 9xj. Should publish strand 1 and strand 2
        Strand strand4 = TestUtil.createStrand(new Path(), "9xj",
                TemporalQuantizer.localDateTimeToEpoch(from.plusMinutes(1)),
                TemporalQuantizer.localDateTimeToEpoch(to.plusMinutes(1)), 1.0, 2.0);
        Map<String, Strand> strandMap  = new HashMap<>();
        strandMap.put(strand1.getKey(), strand1);
        strandMap.put(strand2.getKey(), strand2);
        strandMap.put(strand3.getKey(), strand3);
        strandMap.put(strand4.getKey(), strand4);

        StrandRegistry registry = new StrandRegistry(strands -> {
            // intentionally do nothing
        });
        Map<String, Strand> filteredSet = registry.filterStrandsWithPrefix(strandMap, "9xj",
                TemporalQuantizer.localDateTimeToEpoch(to));
        Map<String, Strand> expected = new HashMap<>();
        expected.put(strand1.getKey(), strand1);
        expected.put(strand2.getKey(), strand2);
        assertEquals(expected, filteredSet);
    }

    @Test
    void testStrandPublishing() {
        LocalDateTime from = LocalDateTime.of(2019, 2, 12, 1, 0, 0);
        LocalDateTime to = LocalDateTime.of(2019, 2, 12, 1, 1, 0);
        Strand strand1 = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.0, 2.0);
        // same geohash and temporal bounds, different feature values.
        Strand strand2 = TestUtil.createStrand(new Path(), "9xj", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.1, 2.0);
        // different geohash
        Strand strand3 = TestUtil.createStrand(new Path(), "9xi", TemporalQuantizer.localDateTimeToEpoch(from),
                TemporalQuantizer.localDateTimeToEpoch(to), 1.0, 2.0);
        // increments the timestamp for the prefix 9xj. Should trigger publishing strand 1 and strand 2
        Strand strand4 = TestUtil.createStrand(new Path(), "9xj",
                TemporalQuantizer.localDateTimeToEpoch(from.plusMinutes(1)),
                TemporalQuantizer.localDateTimeToEpoch(to.plusMinutes(1)), 1.0, 2.0);

        StrandRegistry registry = new StrandRegistry(publisherMock);
        registry.add(strand1);
        registry.add(strand2);
        registry.add(strand3);
        registry.add(strand4);

        // strands1 and strands2 should get published - strand4 increments the timestamp
        Set<Strand> expectedOutput = new HashSet<>();
        expectedOutput.add(strand1);
        expectedOutput.add(strand2);
        Mockito.verify(publisherMock, Mockito.times(1)).publish(expectedOutput);

        // check the session termination
        int total = registry.terminateSession();
        assertEquals(4, total);

        expectedOutput.clear();
        expectedOutput.add(strand3);
        expectedOutput.add(strand4);
        Mockito.verify(publisherMock, Mockito.timeout(1)).publish(expectedOutput);
    }
}
