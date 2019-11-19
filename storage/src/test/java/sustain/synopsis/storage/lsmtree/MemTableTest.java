package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;

class MemTableTest {

    @Test
    void testAdd() {
        MemTable<LSMTestKey, LSMTestValue> memTable = new MemTable<>(1024); // 1 KB
        Assertions.assertFalse(memTable.add(new LSMTestKey(1), new LSMTestValue(256)));

        // test merging
        LSMTestValue mockedValue = mock(LSMTestValue.class);
        memTable.add(new LSMTestKey(2), mockedValue);
        LSMTestValue secondValue = new LSMTestValue(256);
        memTable.add(new LSMTestKey(2), secondValue);
        verify(mockedValue).merge(secondValue);

        // test memTable max size
        memTable.add(new LSMTestKey(3), new LSMTestValue(256));
        Assertions.assertTrue(memTable.add(new LSMTestKey(4), new LSMTestValue(256)));
    }

    @Test
    void testIsFullIfEntrySizeIsNotAvailable() {
        // check if the size of the memTable is bound by the number of entries if the serialized size of an entry
        // cannot be derived.
        LSMTestValue mockedValue = mock(LSMTestValue.class);
        try {
            doThrow(IOException.class).when(mockedValue).serialize(any());
        } catch (IOException e) {
            e.printStackTrace();
        }
        MemTable<LSMTestKey, LSMTestValue> memTable = new MemTable<>(1024, 200);
        for (int i = 0; i < 200 - 1; i++) {
            Assertions.assertFalse(memTable.add(new LSMTestKey(i), mockedValue));
        }
        Assertions.assertTrue(memTable.add(new LSMTestKey(200), mockedValue));
    }

    @Test
    void testReadOnlyChecks() {
        MemTable<LSMTestKey, LSMTestValue> memTable = new MemTable<>(1024, 200);
        Assertions.assertThrows(RuntimeException.class, memTable::getIterator);
        memTable.setReadOnly();
        Assertions.assertThrows(RuntimeException.class, () -> memTable.add(new LSMTestKey(1), new LSMTestValue(128)));
    }

    @Test
    void testIterator() {
        MemTable<LSMTestKey, LSMTestValue> memTable = new MemTable<>(1024, 200);
        LSMTestValue[] values = new LSMTestValue[5];
        for (int i = 5; i > 0; i--) {
            values[i - 1] = new LSMTestValue(64);
            memTable.add(new LSMTestKey(i), values[i - 1]);
        }
        memTable.setReadOnly();

        TableIterator<LSMTestKey, LSMTestValue> iterator = memTable.getIterator();

        int i = 1;
        while (i <= 5) {
            Assertions.assertTrue(iterator.hasNext());
            TableIterator.TableEntry<LSMTestKey, LSMTestValue> entry = iterator.next();
            Assertions.assertEquals(new LSMTestKey(i), entry.getKey());
            Assertions.assertEquals(values[i - 1], entry.getValue());
            i++;
        }
    }
}