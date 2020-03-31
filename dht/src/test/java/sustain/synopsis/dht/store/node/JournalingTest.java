package sustain.synopsis.dht.store.node;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class JournalingTest {
    @Test
    void testCreateEntityStoreActivitySerialization() throws IOException {
        CreateEntityStoreActivity activity = new CreateEntityStoreActivity("dataset_1", "134");
        byte[] serialized = activity.serialize();
        CreateEntityStoreActivity deserializedActivity = new CreateEntityStoreActivity();
        deserializedActivity.deserialize(serialized);
        Assertions.assertEquals("dataset_1", deserializedActivity.getDataSetId());
        Assertions.assertEquals("134", deserializedActivity.getEntityId());
    }
}