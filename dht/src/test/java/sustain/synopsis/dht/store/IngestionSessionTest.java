package sustain.synopsis.dht.store;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import sustain.synopsis.storage.lsmtree.Metadata;

import java.io.*;

public class IngestionSessionTest {

    @Mock
    Metadata<StrandStorageKey> mockedMetadata;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    void testSerialization() throws IOException, IllegalAccessException, InstantiationException {
        IngestionSession session = new IngestionSession("bob", 1391216400000L, 102L);
        session.addSerializedSSTable(mockedMetadata);
        session.setComplete();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        // mock the serialization code - otherwise deserialization fails
        Mockito.doAnswer(invocationOnMock -> {
            DataOutputStream dos1 = invocationOnMock.getArgument(0, DataOutputStream.class);
            dos1.writeLong(0);
            dos1.writeLong(1);
            dos1.writeLong(0);
            dos1.writeLong(1);
            dos1.writeUTF("/test/path");
            dos1.writeInt(0);
            dos1.writeInt(0);
            return null;
        }).when(mockedMetadata).serialize(dos);
        session.serialize(dos);

        // we have separate tests to verify the correctness of metadata - just check if the correct method is called
        Mockito.verify(mockedMetadata, Mockito.times(1)).serialize(dos);
        dos.flush();
        baos.flush();
        byte[] serializedSession = baos.toByteArray();


        ByteArrayInputStream bais = new ByteArrayInputStream(serializedSession);
        DataInputStream dis = new DataInputStream(bais);
        IngestionSession deserializedSession = IngestionSession.deserialize(dis);

        Assertions.assertEquals("bob", deserializedSession.getIngestionUser());
        Assertions.assertEquals(1391216400000L, deserializedSession.getIngestionTime());
        Assertions.assertEquals(102L, deserializedSession.getSessionId());
        Assertions.assertTrue(deserializedSession.isComplete());
        Assertions.assertEquals(1, deserializedSession.getSerializedSSTables().size());
    }
}
