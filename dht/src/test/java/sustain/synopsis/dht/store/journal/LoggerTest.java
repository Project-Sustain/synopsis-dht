package sustain.synopsis.dht.store.journal;

import junit.framework.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;
import sustain.synopsis.dht.journal.AbstractActivity;
import sustain.synopsis.dht.journal.Logger;
import sustain.synopsis.dht.store.StorageException;
import sustain.synopsis.storage.lsmtree.ChecksumGenerator;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class LoggerTest {
    @TempDir
    File root;

    @Test
    void testSerializationAndDeserialization() throws IOException, StorageException {
        File f = new File(root.getAbsolutePath() + File.separator + "journal_log");
        Logger log = new Logger(f.getAbsolutePath());
        List<byte[]> payloads = new ArrayList<>();
        payloads.add("activity1".getBytes());
        payloads.add("activity2".getBytes());
        log.append(payloads.get(0)); // write without initializing - append should call initialize
        log.append(payloads.get(1));
        log.close();

        Assertions.assertTrue(f.length() > 0);
        Logger deserializedLog = new Logger(f.getAbsolutePath());
        Assertions.assertTrue(f.length() > 0);
        Iterator<byte[]> iter = deserializedLog.iterator();
        for (int i = 0; i < payloads.size(); i++) {
            Assertions.assertTrue(iter.hasNext());
            Assertions.assertArrayEquals(payloads.get(i), iter.next());
        }
        Assertions.assertFalse(iter.hasNext());
    }

    @Test
    void testAppendToExisingFile() throws StorageException, IOException {
        File f = new File(root.getAbsolutePath() + File.separator + "journal_log");
        Logger log = new Logger(f.getAbsolutePath());
        log.append("activity1".getBytes());
        log.append("activity2".getBytes());
        log.close();

        log = new Logger(f.getAbsolutePath());
        log.append("activity3".getBytes());

        Iterator<byte[]> iterator = log.iterator();
        for (int i = 1; i <= 3; i++) {
            Assertions.assertTrue(iterator.hasNext());
            Assertions.assertArrayEquals(("activity" + i).getBytes(), iterator.next());
        }
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testChecksumValidation() throws IOException, StorageException, ChecksumGenerator.ChecksumError {
        File f = new File(root.getAbsolutePath() + File.separator + "journal_log");
        Logger log = new Logger(f.getAbsolutePath());
        List<byte[]> payloads = new ArrayList<>();
        payloads.add("activity1".getBytes());
        payloads.add("activity2".getBytes());
        log.append(payloads.get(0)); // write without initializing - append should call initialize
        log.append(payloads.get(1));
        log.close();

        ChecksumGenerator checksumGeneratorMock = mock(ChecksumGenerator.class);
        // first checksum validation fails, second validation is successful
        Mockito.when(checksumGeneratorMock.validateChecksum(any(), any())).thenReturn(false).thenReturn(true);
        Logger deserializedLog = new Logger(f.getAbsolutePath(), checksumGeneratorMock);
        Iterator<byte[]> iterator = deserializedLog.iterator();
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertNull(iterator.next());
        Assertions.assertTrue(iterator.hasNext());
        Assertions.assertArrayEquals(payloads.get(1), iterator.next());
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testIteratorForEmptyFile() {
        File f = new File(root.getAbsolutePath() + File.separator + "journal_log");
        Logger log = new Logger(f.getAbsolutePath());
        Iterator<byte[]> iterator = log.iterator();
        Assertions.assertNotNull(iterator);
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testIteratorForNonExistingFile() {
        Logger log = new Logger(root.getAbsolutePath() + File.separator + "non_existing_file_log");
        Iterator<byte[]> iterator = log.iterator();
        Assertions.assertNotNull(iterator);
        Assertions.assertFalse(iterator.hasNext());
    }

    @Test
    void testAbstractActivity() throws IOException {
        class TestActivity extends AbstractActivity {
            short type = 0;
            int member = 10;

            @Override
            public void serializeMembers(DataOutputStream dataOutputStream) throws IOException {
                dataOutputStream.writeInt(member);
            }

            @Override
            public void deserializeMembers(DataInputStream dataInputStream) throws IOException {
                this.member = dataInputStream.readInt();
            }

            @Override
            public short getType() {
                return type;
            }

        }
        TestActivity testActivity = new TestActivity();
        byte[] serialized = testActivity.serialize();
        TestActivity deserialized = new TestActivity();
        deserialized.deserialize(serialized);
        Assertions.assertEquals(testActivity.getType(), deserialized.getType());
        Assert.assertEquals(testActivity.member, deserialized.member);
    }
}
