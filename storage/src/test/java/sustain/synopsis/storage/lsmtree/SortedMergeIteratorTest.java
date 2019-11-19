package sustain.synopsis.storage.lsmtree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Random;

class SortedMergeIteratorTest {

    @Test
    void testSingleIteratorMerge() {
        // this test covers conversion from a memtable to an sstable
        TableIterator<LSMTestKey, LSMTestValue> iterator = TestUtil.getIterator(0, 1, 5, 10, new Random());
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIterator =
                new SortedMergeIterator<>(Collections.singletonList(iterator));
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
        TableIterator<LSMTestKey, LSMTestValue> iter = TestUtil.getIterator(0, 1, 0, 10, new Random());
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIterator =
                new SortedMergeIterator<>(Collections.singletonList(iter));
        Assertions.assertFalse(mergeIterator.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, mergeIterator::next);
    }

    @Test
    void testMerge() {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIterator =
                new SortedMergeIterator<>(Arrays.asList(TestUtil.getIterator(0, 5, 2, 10, new Random()), // 0,5
                TestUtil.getIterator(1, 1, 4, 10, new Random()), // 1,2,3,4
                TestUtil.getIterator(6, 1, 4, 10, new Random()), // 6,7,8,9
                TestUtil.getIterator(0, 1, 0, 10, new Random()) // empty
        ));

        for (int i = 0; i < 10; i++) {
            Assertions.assertTrue(mergeIterator.hasNext());
            Assertions.assertEquals(new LSMTestKey(i), mergeIterator.next().getKey());
        }

        Assertions.assertFalse(mergeIterator.hasNext());
        Assertions.assertThrows(NoSuchElementException.class, mergeIterator::next);
    }

    @Test
    void testMergeWithDuplicates() {
        SortedMergeIterator<LSMTestKey, LSMTestValue> mergeIterator =
                new SortedMergeIterator<>(Arrays.asList(TestUtil.getIterator(0, 2, 5, 10, new Random()), // 0,2,4,6,8
                TestUtil.getIterator(0, 1, 10, 10, new Random())   // 0,1,2,3,4,...,9
        ));
        int[] elements = new int[]{0, 0, 1, 2, 2, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9};
        for (int element : elements) {
            Assertions.assertEquals(new LSMTestKey(element), mergeIterator.next().getKey());
        }
    }
}
