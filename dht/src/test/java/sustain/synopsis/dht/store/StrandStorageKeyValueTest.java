package sustain.synopsis.dht.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.common.CommonUtil;
import sustain.synopsis.common.ProtoBuffSerializedStrand;
import sustain.synopsis.common.Strand;
import sustain.synopsis.sketch.dataset.feature.Feature;
import sustain.synopsis.sketch.graph.DataContainer;
import sustain.synopsis.sketch.graph.Path;
import sustain.synopsis.sketch.stat.RunningStatisticsND;

import java.io.*;
import java.util.Collections;
import java.util.List;

public class StrandStorageKeyValueTest {
    private long from = 1391216400000L;
    private long to = from + 100;

    public static Strand createStrand(String geohash, long ts, long to, long sessionId, double... features) {
        Path path = new Path(features.length);
        for (int i = 0; i < features.length; i++) {
            path.add(new Feature("feature_" + (i + 1), features[i]));
        }
        RunningStatisticsND runningStats = new RunningStatisticsND(features);
        DataContainer container = new DataContainer(runningStats);
        path.get(path.size() - 1).setData(container);
        return new Strand(geohash, ts, to, path);
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
        return CommonUtil.strandToProtoBuff(strand).toByteArray();
    }

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
    void testStrandStorageKeySerialization() throws IOException {
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
        Strand strand1 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        ProtoBuffSerializedStrand protoBuffSerializedStrand1 = CommonUtil.strandToProtoBuff(strand1);

        StrandStorageValue value1 = new StrandStorageValue(protoBuffSerializedStrand1.toByteArray());
        Assertions.assertEquals(Collections.singletonList(strand1), value1.getStrands());
        Assertions.assertEquals(Collections.singletonList(protoBuffSerializedStrand1), value1.getProtoBuffSerializedStrands());

        // test merging
        Strand strand2 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        ProtoBuffSerializedStrand protoBuffSerializedStrand2 = CommonUtil.strandToProtoBuff(strand2);
        StrandStorageValue value2 = new StrandStorageValue(protoBuffSerializedStrand2.toByteArray());
        value1.merge(value2);

        List<ProtoBuffSerializedStrand> returnedProtoBuffSerializedStrands = value1.getProtoBuffSerializedStrands();
        Assertions.assertEquals(2, returnedProtoBuffSerializedStrands.size());
        Assertions.assertTrue(returnedProtoBuffSerializedStrands.contains(protoBuffSerializedStrand1));
        Assertions.assertTrue(returnedProtoBuffSerializedStrands.contains(protoBuffSerializedStrand2));

        List<Strand> returnedStrands = value1.getStrands();
        Assertions.assertEquals(2, returnedStrands.size());
        Assertions.assertTrue(returnedStrands.contains(strand1));
        Assertions.assertTrue(returnedStrands.contains(strand2));
    }

    @Test
    void testStrandStorageValueSerialization() throws IOException {
        Strand strand1 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        Strand strand2 = createStrand("9xa", from, to, 1, 1.0, 2.0, 3.0);
        StrandStorageValue val = new StrandStorageValue(serializeStrand(strand1));
        val.merge(new StrandStorageValue(serializeStrand(strand2)));

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
            List<Strand> returnedStrands = deserialized.getStrands();
            Assertions.assertEquals(2, returnedStrands.size());
            Assertions.assertTrue(returnedStrands.contains(strand1));
            Assertions.assertTrue(returnedStrands.contains(strand2));
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
    void testStrandStorageKeyGetters() {
        StrandStorageKey key = new StrandStorageKey(10, 20);
        Assertions.assertEquals(10, key.getStartTS());
        Assertions.assertEquals(20, key.getEndTS());
    }
}
