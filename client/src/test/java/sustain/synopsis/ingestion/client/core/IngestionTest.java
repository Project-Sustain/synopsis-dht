package sustain.synopsis.ingestion.client.core;

import org.junit.jupiter.api.Assertions;
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
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

class IngestionTest {

    @Mock
    private StrandRegistry registryMock;

    @BeforeEach
    void setUp(){
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void testRecordToStrandConversion(){
        Map<String, Quantizer> quantizers = new HashMap<>();
        quantizers.put("feature_1", new Quantizer(new Feature(0.0), new Feature(10.0), new Feature(20.0)));
        quantizers.put("feature_2", new Quantizer(new Feature(100.0), new Feature(200.0), new Feature(1000.0)));

        TemporalQuantizer temporalQuantizer = new TemporalQuantizer(Duration.ofHours(1));
        IngestionTask ingestionTask = new IngestionTask(null,null, quantizers, temporalQuantizer);
        Record record = new Record();
        record.setGeohash("9xj");
        long ts = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, 2, 12, 1, 23));
        record.setTimestamp(ts);
        record.addFeatureValue("feature_1", 3.0);
        record.addFeatureValue("feature_2", 140.0);
        Strand strand = ingestionTask.convertToStrand(record);

        //  check the spatio-temporal attributes
        long[] boundaries = temporalQuantizer.getTemporalBoundaries(ts);
        Assertions.assertEquals(boundaries[0], strand.getFromTimeStamp());
        Assertions.assertEquals(boundaries[1], strand.getToTimestamp());
        Assertions.assertEquals("9xj", strand.getGeohash());

        // check the path
        Path path = strand.getPath();
        Assertions.assertEquals("feature_1",  path.get(0).getLabel().getName());
        Assertions.assertEquals(0.0,  path.get(0).getLabel().getDouble()); // discretized value

        Assertions.assertEquals("feature_2",  path.get(1).getLabel().getName());
        Assertions.assertEquals(100.0,  path.get(1).getLabel().getDouble()); // discretized value
        Assertions.assertNotNull(path.get(1).getData()); // check the data container

        // add a new feature that does not have a quantizer
        record.addFeatureValue("feature_3", 3.01);
        Assertions.assertThrows(AssertionError.class, () -> {ingestionTask.convertToStrand(record);});
    }

    @Test
    void testIngestionTaskExecution(){
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
}
