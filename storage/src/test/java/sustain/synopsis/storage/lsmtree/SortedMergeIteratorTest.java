package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;

class MergeIteratorTest {

    /**
     * Generate a {@link TableIterator} with keys start, start + 1 * step size, ... ,
     * start + (count - 1) * step size
     *
     * @param start    Starting element
     * @param stepSize Increment size
     * @param count    Number of elements produced by the iterator
     * @return {@link TableIterator} instance
     */
    private TableIterator<LSMTestKey, LSMTestValue> getIterator(int start, int stepSize, int count) {
        return new TableIterator<LSMTestKey, LSMTestValue>() {
            int i = 0;

            @Override
            public boolean hasNext() {
                return i < count;
            }

            @Override
            public TableEntry<LSMTestKey, LSMTestValue> next() {
                return new TableEntry<>(new LSMTestKey(start + i++ * stepSize), new LSMTestValue(1));
            }

            @Override
            public long count() {
                return count;
            }

            @Override
            public long estimatedSize() {
                return count * (4 + 4 + 1);
            }
        };
    }

    @Test
    void testSingleIteratorMerge() {
        // this test covers conversion from a memtable to an sstable
        TableIterator<LSMTestKey, LSMTestValue> iterator = getIterator(0, 1, 5);
        MergeIterator<LSMTestKey, LSMTestValue> mergeIterator =
                new MergeIterator<>(Collections.singletonList(iterator));
        Assertions.assertEquals(5, mergeIterator.count());
        Assertions.assertEquals(5 * (4 + 4 + 1), mergeIterator.estimatedSize());
        // check if the elements are sorted
        for (int i = 0; i < 5; i++) {
            Assertions.assertTrue(mergeIterator.hasNext());
            Assertions.assertEquals(new LSMTestKey(i), mergeIterator.next().getKey());
        }

        // check the number of elements returned by the iterator
        Assertions.assertFalse(mergeIterator.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, mergeIterator::next);
    }

    @Test
    void testEmptySingleIterator() {
        TableIterator<LSMTestKey, LSMTestValue> iter = getIterator(0, 1, 0);
        MergeIterator<LSMTestKey, LSMTestValue> mergeIterator = new MergeIterator<>(Collections.singletonList(iter));
        Assertions.assertEquals(0, mergeIterator.count());
        Assertions.assertEquals(0, mergeIterator.estimatedSize());
        Assertions.assertFalse(mergeIterator.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, mergeIterator::next);
    }

    @Test
    void testMerge() {
        MergeIterator<LSMTestKey, LSMTestValue> mergeIterator = new MergeIterator<>(Arrays.asList(getIterator(0, 5,
                2), // 0,5
                getIterator(1, 1, 4), // 1,2,3,4
                getIterator(6, 1, 4), // 6,7,8,9
                getIterator(0, 1, 0) // empty
        ));
        Assertions.assertEquals(10, mergeIterator.count());
        Assertions.assertEquals(10 * (4 + 4 + 1), mergeIterator.estimatedSize());

        for (int i = 0; i < 10; i++) {
            Assertions.assertTrue(mergeIterator.hasNext());
            Assertions.assertEquals(new LSMTestKey(i), mergeIterator.next().getKey());
        }

        Assertions.assertFalse(mergeIterator.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, mergeIterator::next);
    }

    @Test
    void testMergeWithDuplicates() {
        MergeIterator<LSMTestKey, LSMTestValue> mergeIterator = new MergeIterator<>(Arrays.asList(getIterator(0, 2,
                5), // 0,2,4,6,8
                getIterator(0, 1, 10)   // 0,1,2,3,4,...,9
        ));
        Assertions.assertEquals(15, mergeIterator.count());
        Assertions.assertEquals(15 * (4 + 4 + 1), mergeIterator.estimatedSize());
        int[] elements = new int[]{0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9};
        for (int element : elements) {
            Assertions.assertEquals(new LSMTestKey(element), mergeIterator.next().getKey());
        }
    }
}
