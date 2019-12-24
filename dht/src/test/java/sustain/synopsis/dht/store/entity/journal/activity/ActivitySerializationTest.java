package sustain.synopsis.dht.store.entity.journal.activity;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sustain.synopsis.dht.journal.Activity;

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
}
