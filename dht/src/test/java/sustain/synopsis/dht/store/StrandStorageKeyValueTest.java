package sustain.synopsis.dht.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.serialization.SerializationOutputStream;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.io.*;

public class StrandStorageKeyValueTest {
    private long from = 1391216400000L;
    private long to = from + 100;

    @Test
    void testStrandStorageKeyCompare() {
        StrandStorageKey key = new StrandStorageKey(from, to);
        StrandStorageKey greaterKey = new StrandStorageKey(from + 10, to);
        StrandStorageKey lowerKey = new StrandStorageKey(from - 10, from);
        StrandStorageKey equalKey = new StrandStorageKey(from, to);
        Assertions.assertTrue(key.compareTo(greaterKey) < 0);
        Assertions.assertTrue(key.compareTo(lowerKey) > 0);
        Assertions.assertEquals(0, key.compareTo(equalKey));
        Assertions.assertEquals(key, equalKey);
        Assertions.assertEquals(key.hashCode(), equalKey.hashCode());
    }

    @Test
    void testStrandStorageSerialization() throws IOException {
        StrandStorageKey key = new StrandStorageKey(from, to);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] serialized;
        try {
            key.serialize(dos);
            dos.flush();
            baos.flush();
            serialized = baos.toByteArray();
        } finally {
            dos.close();
            baos.close();
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
        DataInputStream dis = new DataInputStream(bais);
        StrandStorageKey deserializedKey = new StrandStorageKey();
        try {
            deserializedKey.deserialize(dis);
            Assertions.assertEquals(key, deserializedKey);
        } finally {
            bais.close();
            dis.close();
        }
    }

    @Test
    void testStrandStorageValueMerge() throws IOException {
        Strand strand1 = createStrand("9xa", from, to, 1.0, 2.0, 3.0);
        Strand strand2 = createStrand("9xa", from, to, 1.0, 2.0, 3.0);
        StrandStorageValue value1 = new StrandStorageValue(serializeStrand(strand1));
        StrandStorageValue value2 = new StrandStorageValue(serializeStrand(strand2));
        value1.merge(value2);
        strand1.merge(strand2);
        Assertions.assertEquals(value1.getStrand(), strand1);
    }

    @Test
    void testStrandSerialization() throws IOException {
        Strand strand = createStrand("9xa", from, to, 1.0, 2.0, 3.0);
        StrandStorageValue val = new StrandStorageValue(serializeStrand(strand));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        byte[] serializedData;
        try {
            val.serialize(dos);
            baos.flush();
            dos.flush();
            serializedData = baos.toByteArray();
        } finally {
            baos.close();
            dos.close();
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(serializedData);
        DataInputStream dis = new DataInputStream(bais);
        StrandStorageValue deserialized = new StrandStorageValue();
        try {
            deserialized.deserialize(dis);
            Assertions.assertEquals(val.getStrand(), deserialized.getStrand());
        } finally {
            bais.close();
            dis.close();
        }
    }

    @Test
    void testStrandStorageKeyToString() {
        StrandStorageKey key = new StrandStorageKey(from, to);
        Assertions.assertEquals(from + "_" + to, key.toString());
    }

    @Test
    void testStrandStorageKeyGetters(){
        StrandStorageKey key = new StrandStorageKey(10, 20);
        Assertions.assertEquals(10, key.getStartTS());
        Assertions.assertEquals(20, key.getEndTS());
    }

    public static Strand createStrand(String geohash, long ts, long to, double... features) {
        Path path = new Path(features.length);
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
    }

    public static byte[] serializeStrand(Strand strand) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); SerializationOutputStream sos =
                new SerializationOutputStream(baos)) {
            strand.serialize(sos);
            sos.flush();
            baos.flush();
            return baos.toByteArray();
        }
    }
}
