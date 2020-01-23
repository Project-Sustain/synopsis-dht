package sustain.synopsis.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.serialization.SerializationException;
import sustain.synopsis.sketch.serialization.SerializationInputStream;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class StrandTest {

    @Test
    void testStrandInitialization() {
        Path path = new Path(2);
        Strand strand = createStrand(path, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L, 1.34, 1.5);

        Assertions.assertEquals("9xj", strand.getGeohash());
        Assertions.assertEquals((1391216400000L - 3600 * 1000), strand.getFromTimeStamp());
        Assertions.assertEquals(1391216400000L, strand.getToTimestamp());
        Assertions.assertEquals(path, strand.getPath());
    }

    @Test
    void testStrandMetadata(){
        Path path = new Path(2);
        Strand strand = createStrand(path, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L, 1.34, 1.5);
        strand.addMetadata("key1", "val1");
        strand.addMetadata("key2", "val2");
        Assertions.assertEquals("val1", strand.getMetadata("key1"));
        Assertions.assertEquals("val2", strand.getMetadata("key2"));
        // test overriding
        strand.addMetadata("key2", "new_val");
        Assertions.assertEquals("new_val", strand.getMetadata("key2"));

        Assertions.assertNull(strand.getMetadata("key3"));
    }

    @Test
    void testStrandMerge() {
        Path path1 = new Path(2);
        Strand strand1 = createStrand(path1, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.5);

        Path path2 = new Path(2);
        Strand strand2 = createStrand(path2, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.5);

        strand1.merge(strand2);

        DataContainer container = strand1.getPath().get(1).getData();
        Assertions.assertEquals(2, container.statistics.count());
    }

    @Test
    void testStrandEquals(){
        Path path = new Path(2);
        Strand strand = createStrand(path, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.5);

        Strand strand2 = new Strand("9xj", (1391216400000L - 3600 * 1000), 1391216400000L, path);
        Assertions.assertEquals(strand, strand2);

        // different geohash
        Strand strand3 = new Strand("9xi", (1391216400000L - 3600 * 1000), 1391216400000L, path);
        Assertions.assertNotEquals(strand, strand3);

        // different from timestamp
        Strand strand4 = new Strand("9xj", (1391216300000L - 3600 * 1000), 1391216400000L, path);
        Assertions.assertNotEquals(strand, strand4);

        // different to timestamp
        Strand strand5 = new Strand("9xj", (1391216400000L - 3600 * 1000), 1391216500000L, path);
        Assertions.assertNotEquals(strand, strand5);

        // different path
        Path path2 = new Path(3);
        Strand strand6 = createStrand(path, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.5, 4.6);
        Assertions.assertNotEquals(strand, strand6);

        // different metadata
        strand2.addMetadata("key1", "value1");
        Assertions.assertNotEquals(strand, strand2);
    }

    @Test
    void testStrandSerialization() throws IOException, SerializationException {
        Path path = new Path(2);
        Strand strand = createStrand(path, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.5);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializationOutputStream sos = new SerializationOutputStream(baos);
        strand.serialize(sos);
        sos.flush();
        baos.flush();
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        SerializationInputStream sis = new SerializationInputStream(bais);
        Strand deserializedStrand = new Strand(sis);
        Assertions.assertEquals(strand, deserializedStrand);
    }

    @Test
    void testStrandMergeExceptions() {
        Path path1 = new Path(2);
        Strand strand1 = createStrand(path1, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.5);

        // different path lengths
        Path path2 = new Path(1);
        final Strand strand2 = createStrand(path2, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            strand1.merge(strand2);
        });

        // different paths
        path2 = new Path(2);
        final Strand strand3 = createStrand(path2, "9xj", (1391216400000L - 3600 * 1000), 1391216400000L,1.34, 1.6);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            strand1.merge(strand3);
        });
    }

    private Strand createStrand(Path path, String geohash, long ts, long to, double... features) {
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
    }
}
