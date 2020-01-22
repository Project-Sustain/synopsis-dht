package sustain.synopsis.dht.store.entity.journal.activity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.dht.journal.Activity;
import sustain.synopsis.dht.store.StrandStorageKey;
import sustain.synopsis.dht.store.entity.journal.JournalLogFactory;
import sustain.synopsis.dht.store.entity.journal.JournalingException;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.IOException;

public class ActivitySerializationTest {
    @Test
    void testStartSessionActivity() throws IOException {
        Activity sessionActivity = new StartSessionActivity("bob", 12345L, 100032L);
        byte[] arr = sessionActivity.serialize();

        StartSessionActivity deserializedSessionActivity = new StartSessionActivity();
        deserializedSessionActivity.deserialize(arr);
        Assertions.assertEquals("bob", deserializedSessionActivity.getUser());
        Assertions.assertEquals(12345L, deserializedSessionActivity.getIngestionTimeStamp());
        Assertions.assertEquals(100032L, deserializedSessionActivity.getSessionId());
    }

    @Test
    void testSerializeSSTableActivity() throws IOException{
        Metadata<StrandStorageKey> metadata = new Metadata<>();
        metadata.setMin(new StrandStorageKey(10L, 15L));
        metadata.setMax(new StrandStorageKey(30L, 35L));
        metadata.setPath("/test/path");
        SerializeSSTableActivity serializeActivity = new SerializeSSTableActivity(123L, metadata);
        byte[] serialized = serializeActivity.serialize();

        SerializeSSTableActivity deserialized = new SerializeSSTableActivity();
        deserialized.deserialize(serialized);
        Assertions.assertEquals(123L, deserialized.getSessionId());
        Metadata<StrandStorageKey> deserializedMetadata = deserialized.getMetadata();
        Assertions.assertEquals(new StrandStorageKey(10L, 15L), deserializedMetadata.getMin());
        Assertions.assertEquals(new StrandStorageKey(30L, 35L), deserializedMetadata.getMax());
    }

    @Test
    void testSerializeIncSeqIdActivity() throws IOException{
        IncSeqIdActivity activity = new IncSeqIdActivity(1);
        byte[] serialized = activity.serialize();
        IncSeqIdActivity deserializedActivity = new IncSeqIdActivity();
        deserializedActivity.deserialize(serialized);
        Assertions.assertEquals(1, deserializedActivity.getSequenceId());
    }

    @Test
    void testEndSession() throws IOException {
        EndSessionActivity endSessionActivity = new EndSessionActivity(123L);
        byte[] serialized = endSessionActivity.serialize();

        EndSessionActivity deserialized = new EndSessionActivity();
        deserialized.deserialize(serialized);

        Assertions.assertEquals(123L, deserialized.getSessionId());
    }

    @Test
    void testJournalLogFactory() throws IOException {
        try {
            StartSessionActivity startSessionActivity = new StartSessionActivity("bob", 1234L, 567L);
            Activity activity = JournalLogFactory.parse(startSessionActivity.serialize());
            Assertions.assertEquals(StartSessionActivity.TYPE, activity.getType());

            Metadata<StrandStorageKey> metadata = new Metadata<>();
            metadata.setMin(new StrandStorageKey(10L, 15L));
            metadata.setMax(new StrandStorageKey(30L, 35L));
            metadata.setPath("/test/path");
            SerializeSSTableActivity serializeSSTableActivity = new SerializeSSTableActivity(124L, metadata);
            activity = JournalLogFactory.parse(serializeSSTableActivity.serialize());
            Assertions.assertEquals(SerializeSSTableActivity.TYPE, activity.getType());

            EndSessionActivity endSessionActivity = new EndSessionActivity(1224L);
            activity = JournalLogFactory.parse(endSessionActivity.serialize());
            Assertions.assertEquals(EndSessionActivity.TYPE, activity.getType());

            // check if deserialize method is called
            Assertions.assertEquals(1224L, ((EndSessionActivity)activity).getSessionId());

            IncSeqIdActivity incrementSeqIdActivity = new IncSeqIdActivity(1);
            activity= JournalLogFactory.parse(incrementSeqIdActivity.serialize());
            Assertions.assertEquals(IncSeqIdActivity.class, activity.getClass());
        } catch (JournalingException ignore) {

        }

    }
}
