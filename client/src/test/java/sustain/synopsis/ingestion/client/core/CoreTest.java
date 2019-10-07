package sustain.synopsis.ingestion.client.core;

import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.temporal.Temporal;

public class CoreTest {

    private Strand createStrand(Path path, String geohash, long ts, double ...features){
        path.add(new Feature("location", geohash));
        path.add(new Feature("time", ts));
        StringBuilder keyBuilder = new StringBuilder();
        keyBuilder.append(geohash).append(ts);
        for (int i = 0; i< features.length; i++){
            path.add(new Feature("feature_" + (i + 1), features[i]));
            keyBuilder.append(features[i]);
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size()-1).setData(container);
        return new Strand("9xj", 1391216400000L, path, keyBuilder.toString());
    }

    @Test
    void testStrandInitialization(){
        Path path = new Path(4);
        Strand strand = createStrand(path, "9xj", 1391216400000L, 1.34, 1.5);

        Assertions.assertEquals("9xj", strand.getGeohash());
        Assertions.assertEquals(1391216400000L, strand.getTimestamp());
        Assertions.assertEquals(path, strand.getPath());
    }

    @Test
    void testStrandMerge(){
        Path path1 = new Path(4);
        Strand strand1 = createStrand(path1,"9xj", 1391216400000L, 1.34, 1.5);

        Path path2 = new Path(4);
        Strand strand2 = createStrand(path2,"9xj", 1391216400000L, 1.34, 1.5);

        strand1.merge(strand2);

        DataContainer container = strand1.getPath().get(3).getData();
        Assertions.assertEquals(2, container.statistics.count());
    }

    @Test
    void testStrandMergeExceptions(){
        Path path1 = new Path(4);
        Strand strand1 = createStrand(path1,"9xj", 1391216400000L, 1.34, 1.5);

        // different path lengths
        Path path2 = new Path(3);
        final Strand strand2 = createStrand(path2, "9xj", 1391216400000L, 1.34);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {strand1.merge(strand2);});

        // different paths
        path2 = new Path(4);
        final Strand strand3 = createStrand(path2, "9xj", 1391216400000L, 1.34, 1.6);
        Assertions.assertThrows(IllegalArgumentException.class, ()->{strand1.merge(strand3);});
    }

    @Test
    void testTemporalQuantizer(){
        TemporalQuantizer quantizer = new TemporalQuantizer(Duration.ofHours(1));
        LocalDateTime ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 6, 21, 30);
        long returned = quantizer.getBoundary( quantizer.getBoundary(TemporalQuantizer.localDateTimeToEpoch(ts1)));
        long expectedBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY,
                12, 7, 0, 0));
        Assertions.assertEquals(expectedBoundary, returned);

        // simulate an out of order record - now the boundary is advanced to 07:00
        ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 3, 21, 30);
        returned = quantizer.getBoundary( quantizer.getBoundary(TemporalQuantizer.localDateTimeToEpoch(ts1)));
        expectedBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY,
                12, 4, 0, 0));
        System.out.println(TemporalQuantizer.epochToLocalDateTime(returned));
        Assertions.assertEquals(expectedBoundary, returned);

        // simulate a sparse observation
        ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 17, 21, 30);
        returned = quantizer.getBoundary( quantizer.getBoundary(TemporalQuantizer.localDateTimeToEpoch(ts1)));
        expectedBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY,
                12, 18, 0, 0));
        Assertions.assertEquals(expectedBoundary, returned);

        // check the boundaries

        ts1 = LocalDateTime.of(2019, Month.JANUARY, 12, 18, 0, 0);
        returned = quantizer.getBoundary( quantizer.getBoundary(TemporalQuantizer.localDateTimeToEpoch(ts1)));
        expectedBoundary = TemporalQuantizer.localDateTimeToEpoch(LocalDateTime.of(2019, Month.JANUARY,
                12, 18, 0, 0));
        System.out.println(TemporalQuantizer.epochToLocalDateTime(returned));
        Assertions.assertEquals(expectedBoundary, returned);
    }
}
